# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08b_security_setup - row-level security, column masking, dynamic view and grants (PDF Day 8-9)
# MAGIC
# MAGIC **Goal:** analysts see only the states of their team and rounded money values; the data-engineering
# MAGIC group sees everything. Rules come from `conf/vstone_config.json -> security`:
# MAGIC
# MAGIC | Group | Rows | Money columns |
# MAGIC |---|---|---|
# MAGIC | `vstone_admins` | all states | exact |
# MAGIC | `vstone_analysts_west` | CA, OR, WA, NV, AZ | rounded to 1,000 |
# MAGIC | `vstone_analysts_east` | NY, NJ, MA, PA, FL | rounded to 1,000 |
# MAGIC
# MAGIC | Technique | Object |
# MAGIC |---|---|
# MAGIC | Access map | `governance.state_access_map` |
# MAGIC | Row-level security (row filter function) | `governance.rf_state_access` on `gold.market_trend_secured(state_code)` |
# MAGIC | Column-level security (column mask function) | `governance.mask_usd_value` on 3 USD columns |
# MAGIC | Dynamic view | `gold.v_state_sales_by_access` (row rule in WHERE + a hidden column) |
# MAGIC | Grants | analyst groups: USE CATALOG/SCHEMA + SELECT on the secured objects only |
# MAGIC
# MAGIC **One-time prerequisite:** create the 3 groups in Settings -> Identity and access -> Groups, add yourself to
# MAGIC `vstone_admins` and the invited second user to an analyst group (see `docs/security_rls_cls.md`).

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
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")

ctx = get_context(dbutils)

# COMMAND ----------

from vstone_framework import security as S
from vstone_framework.config import load_config
from vstone_framework.uc import set_tags

cfg = load_config()
sec, T = cfg["security"], cfg["gold"]["tables"]
access_table = f"{ctx.governance}.{sec['access_table']}"
rf, mask = f"{ctx.governance}.{sec['row_filter_function']}", f"{ctx.governance}.{sec['mask_function']}"
secured = f"{ctx.gold}.{sec['secured_table']}"
view = f"{ctx.gold}.{sec['dynamic_view']}"

def run(sql):
    print(sql if len(sql) < 300 else sql[:300] + " ...")
    spark.sql(sql)

# COMMAND ----------

# MAGIC %md ## 1. Access map (group -> states)

# COMMAND ----------

run(S.access_table_ddl(access_table))
spark.createDataFrame(S.access_map_rows(sec["state_access"]), "group_name STRING, state_code STRING") \
     .write.insertInto(access_table, overwrite=True)
set_tags(spark, access_table, {**cfg["tags"], "layer": "governance", "entity": "access_map"})
display(spark.table(access_table))

# COMMAND ----------

# MAGIC %md ## 2. Row filter and column mask functions

# COMMAND ----------

run(S.row_filter_function_sql(rf, access_table, sec["admin_group"]))
run(S.mask_function_sql(mask, sec["admin_group"], int(sec["mask_round_digits"])))

# COMMAND ----------

# MAGIC %md ## 3. Secured table: row filter (RLS) + column masks (CLS)

# COMMAND ----------

run(S.secured_table_sql(secured, f"{ctx.gold}.{T[sec['secured_source']]}",
                        f"Gold (secured): {T[sec['secured_source']]} with row-level security on {sec['filter_column']} "
                        f"and masks on {', '.join(sec['masked_columns'])}"))
for sql in S.apply_policies_sql(secured, rf, sec["filter_column"], mask, sec["masked_columns"]):
    run(sql)
set_tags(spark, secured, {**cfg["tags"], "layer": "gold", "entity": "secured_market_trend", "security": "rls_cls"})

# COMMAND ----------

# MAGIC %md ## 4. Dynamic view

# COMMAND ----------

source = f"{ctx.gold}.{T[sec['dynamic_view_source']]}"
run(S.dynamic_view_sql(view, source, spark.table(source).columns, sec["filter_column"], access_table,
                       sec["admin_group"], sec["dynamic_view_hidden_columns"]))

# COMMAND ----------

# MAGIC %md ## 5. Grants (least privilege for analyst groups)

# COMMAND ----------

grant_errors = []
for sql in S.grant_sql(ctx.catalog, [ctx.gold_schema], [secured, view],
                       S.analyst_groups(sec["groups"], sec["admin_group"])):
    try:
        run(sql)
    except Exception as e:                       # group not created yet -> report clearly, do not hide it
        grant_errors.append(f"{sql}: {str(e).splitlines()[0]}")
if grant_errors:
    raise RuntimeError("Grants failed - create the groups in Settings -> Identity and access -> Groups:\n"
                       + "\n".join(grant_errors))

# COMMAND ----------

# MAGIC %md ## 6. Evidence: policies attached, and what the current user sees

# COMMAND ----------

# DESCRIBE TABLE EXTENDED lists the row filter and the column masks attached to the table
policies = spark.sql(f"DESCRIBE TABLE EXTENDED {secured}")
display(policies)          # scroll to "Row Filter" and the "Column Masks" section
me = spark.sql("SELECT current_user() AS u").first().u
print(f"Current user: {me}")
display(spark.sql(f"""SELECT state_code, COUNT(*) AS months, ROUND(MAX(home_value_usd), 2) AS max_home_value_usd
                      FROM {secured} WHERE state_code IS NOT NULL GROUP BY state_code ORDER BY state_code"""))
display(spark.sql(f"SELECT state_code, COUNT(*) AS years FROM {view} GROUP BY state_code ORDER BY state_code"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Built the access map and the row filter / column mask functions from the config file.
# MAGIC - Published `gold.market_trend_secured` with row-level security on `state_code` and masks on 3 USD columns.
# MAGIC - Published the dynamic view `gold.v_state_sales_by_access` (row rule + hidden sale price for non-admins).
# MAGIC - Granted analyst groups only USE + SELECT on the secured objects; showed the attached policies.
