# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08b_security_setup - tiered access model: RLS, column masks, dynamic view, grants (PDF Day 8-9)
# MAGIC
# MAGIC **Goal:** every person sees exactly what their level allows - decided in **one place**
# MAGIC (`governance.fn_access_level()`) and enforced by Unity Catalog on every query.
# MAGIC
# MAGIC | Level (groups) | Rows | Money (USD) | Rent | Growth % | ZIP code | Lineage |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | admin (`vstone_admins`) | all states | exact | exact | shown | full | shown |
# MAGIC | analyst (`vstone_analysts_west/east`) | own states | rounded to 1,000 | rounded to 10 | shown | full (known limit: all ZIPs) | REDACTED |
# MAGIC | viewer (`vstone_viewers`) | all states | rounded to 10,000 | rounded to 100 | hidden | first 3 digits, e.g. `940**` | REDACTED |
# MAGIC | none (anyone else) | none | - | - | - | - | - |
# MAGIC
# MAGIC | Step | Objects |
# MAGIC |---|---|
# MAGIC | 1 Policy decision point | `governance.fn_access_level()` |
# MAGIC | 2 Mask rules (pure + column masks) | `governance.mask_<rule>_for_level`, `governance.mask_<rule>` for usd, pct, zip, internal |
# MAGIC | 3 Self-test of every rule x level | results in `ops.sanity_check_results` (layer `security`) |
# MAGIC | 4 Row filter + access map | `governance.rf_state_access`, `governance.state_access_map` |
# MAGIC | 5 Secured tables (filter + masks + sensitivity tags) | `gold.market_trend_secured`, `gold.zip_home_values_secured` |
# MAGIC | 6 Dynamic view | `gold.v_state_sales_by_access` |
# MAGIC | 7 Policy catalog (who sees what) | `governance.access_policy_catalog` |
# MAGIC | 8 Grants | all four groups: admin full access, analyst and viewer least privilege |
# MAGIC
# MAGIC Everything comes from `conf/vstone_config.json -> security`. One-time prerequisite: the 4 groups exist
# MAGIC (Settings -> Identity and access -> Groups) - see `docs/security_rls_cls.md`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("gold_schema", d["gold_schema"], "Gold schema")
dbutils.widgets.text("governance_schema", d["governance_schema"], "Governance schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")

ctx = get_context(dbutils)

# COMMAND ----------

from vstone_framework import security as S
from vstone_framework.config import load_config
from vstone_framework.ops import ensure_ops_tables, write_check_results
from vstone_framework.quality import _result, failed
from vstone_framework.uc import set_tags

cfg = load_config()
sec, g = cfg["security"], cfg["gold"]
gov, gold = ctx.governance, ctx.gold
rules = {k: v for k, v in sec["mask_rules"].items() if not k.startswith("_")}
access_fn = f"{gov}.{sec['functions']['access_level']}"
rf = f"{gov}.{sec['functions']['row_filter']}"
access_table = f"{gov}.{sec['access_table']}"
catalog_table = f"{gov}.{sec['policy_catalog_table']}"
mask_fn, for_level_fn = {}, {}
for r in rules:
    mask_fn[r], for_level_fn[r] = S.mask_names(gov, sec["functions"]["mask_prefix"], r)
sensitivity = {r: spec["sensitivity"] for r, spec in rules.items()}
ensure_ops_tables(spark, ctx.ops)

def run(sql):
    print(sql.splitlines()[0][:160])
    spark.sql(sql)

# COMMAND ----------

# MAGIC %md ## 1-2. Policy decision point and mask functions

# COMMAND ----------

run(S.access_level_function_sql(access_fn, sec["groups"], sec["levels"]))
for r, spec in rules.items():
    run(S.mask_for_level_sql(for_level_fn[r], spec))
    run(S.column_mask_sql(mask_fn[r], for_level_fn[r], access_fn, spec))

# COMMAND ----------

# MAGIC %md ## 3. Self-test: every mask rule x every level gives the expected output

# COMMAND ----------

results = []
for t in sec["self_test"]:
    for level, expected in t["expected"].items():
        got = spark.sql(S.self_test_sql(for_level_fn[t["rule"]], t["input"], level)).first().out
        results.append(_result(for_level_fn[t["rule"]], f"{t['rule']}_{level}", expected, got, got == expected))
write_check_results(spark, ctx.ops, ctx.run_id, "security", results)
display(spark.createDataFrame([(r.check_name, str(r.expected), str(r.actual), r.status) for r in results],
                              "check STRING, expected STRING, actual STRING, status STRING"))
if failed(results):
    raise AssertionError("Mask self-test failed: " + str([(r.check_name, r.expected, r.actual) for r in failed(results)]))

# COMMAND ----------

# MAGIC %md ## 4. Access map and row filter

# COMMAND ----------

run(S.access_table_ddl(access_table))
spark.createDataFrame(S.access_map_rows(sec["state_access"]), "group_name STRING, state_code STRING") \
     .write.insertInto(access_table, overwrite=True)
set_tags(spark, access_table, {**cfg["tags"], "layer": "governance", "entity": "access_map"})
run(S.row_filter_function_sql(rf, access_fn, access_table, sec["levels"]))

# COMMAND ----------

# MAGIC %md ## 5. Secured tables: row filter + column masks + sensitivity tags

# COMMAND ----------

secured = []
for name, spec in sec["secured_tables"].items():
    table = f"{gold}.{name}"
    if spec["source"] == "zip_from_fact":
        run(S.zip_table_sql(table, f"{gold}.{g['tables']['fact']}", f"{gold}.{g['tables']['dim_region_current']}",
                            g["home_value_metric"], int(spec["months"]), spec["description"], spec["level"]))
    else:
        run(S.copy_table_sql(table, f"{gold}.{g['tables'][spec['source']]}", spec["description"]))
    for sql in S.apply_policies_sql(table, rf if spec["row_filter"] else None, spec["row_filter"],
                                    spec["masks"], mask_fn, sensitivity):
        run(sql)
    set_tags(spark, table, {**cfg["tags"], "layer": "gold", "entity": name, "security": "tiered_access"})
    secured.append(table)

# COMMAND ----------

# MAGIC %md ## 6. Dynamic view

# COMMAND ----------

dv = sec["dynamic_view"]
view = f"{gold}.{dv['name']}"
source = f"{gold}.{g['tables'][dv['source']]}"
admin_level = sec["groups"][sec["admin_group"]]["level"]
run(S.dynamic_view_sql(view, source, spark.table(source).columns, dv["filter_column"], rf, access_fn, admin_level,
                       dv["hidden_columns"], dv["masked_columns"], for_level_fn, dv["description"]))

# COMMAND ----------

# MAGIC %md ## 7. Policy catalog: who sees what

# COMMAND ----------

spark.createDataFrame(S.policy_catalog_rows(sec, gold, gov), S.POLICY_CATALOG_SCHEMA) \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(catalog_table)
spark.sql(f"COMMENT ON TABLE {catalog_table} IS 'Access matrix generated from config: per object and column, "
          f"the technique and what each access level sees'")
display(spark.table(catalog_table))

# COMMAND ----------

# MAGIC %md ## 8. Least-privilege grants

# COMMAND ----------

view_functions = [access_fn, rf] + [for_level_fn[r] for r in set(dv["masked_columns"].values())]
grant_errors = []
for sql in S.grant_sql(ctx.catalog, ctx.gold_schema, ctx.governance_schema, secured + [view], view_functions,
                       catalog_table, list(sec["groups"])):
    try:
        run(sql)
    except Exception as e:                       # group not created yet -> report clearly
        grant_errors.append(f"{sql}: {str(e).splitlines()[0]}")
if grant_errors:
    raise RuntimeError("Grants failed - create the groups in Settings -> Identity and access -> Groups:\n"
                       + "\n".join(grant_errors))

# COMMAND ----------

# MAGIC %md ## 9. Evidence: your level and what you see

# COMMAND ----------

display(spark.sql(f"SELECT current_user() AS principal, {access_fn}() AS access_level"))
ev = sec["evidence"]
display(spark.sql(f"""SELECT state_code, COUNT(*) AS months, MAX({ev['money']}) AS max_{ev['money']},
                             MAX({ev['pct']}) AS max_{ev['pct']}, MAX({ev['lineage']}) AS {ev['lineage']}
                      FROM {gold}.{ev['table']} WHERE state_code IS NOT NULL
                      GROUP BY state_code ORDER BY state_code LIMIT 10"""))
display(spark.sql(f"SELECT * FROM {gold}.{ev['zip_table']} LIMIT 5"))
display(spark.sql(f"DESCRIBE TABLE EXTENDED {gold}.{ev['table']}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Built one policy decision point (`fn_access_level`) and 4 tiered mask rules, and **self-tested every rule for every level**.
# MAGIC - Secured 2 tables with a row filter, column masks and sensitivity tags, and published a dynamic view.
# MAGIC - Wrote the access matrix to `governance.access_policy_catalog` and granted access to all four groups: admin full access, analyst and viewer least privilege.
# MAGIC - Showed the caller's level and the masked data they see.
