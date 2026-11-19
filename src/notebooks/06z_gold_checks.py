# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06z_gold_checks - sanity checks after the Gold pipeline
# MAGIC **Goal:** prove the star schema is complete and consistent before anyone queries it. Runs after the
# MAGIC Gold DLT pipeline in the **vstone_gold** job; any FAIL fails the job (and the job emails you).
# MAGIC
# MAGIC | Check | Rule |
# MAGIC |---|---|
# MAGIC | row_count_matches_silver | fact rows = rows of `silver.region_metric_monthly` (nothing lost, nothing added) |
# MAGIC | primary_key_unique / not_null | fact (region, metric, month) and every current dimension key |
# MAGIC | `<fk>`_has_dimension_row | every fact key exists in its dimension (referential integrity) |
# MAGIC | scd2_one_current_version_per_key | every key of dim_region (the only Type 2 dimension) has exactly 1 open version |
# MAGIC | all_levels_reach_gold | all 6 geography levels are in the fact (PDF: every non-metadata file reaches Gold) |
# MAGIC | top_n_per_level | the top-10 table has 1-10 rows for every configured level |
# MAGIC | has_rows | the trend and sales aggregates are not empty |
# MAGIC | churn: one_status_per_region, status_in_allowed_values, summary_adds_up | churn table and its summary agree |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Job parameters arrive as widgets; defaults (from `conf/vstone_config.json`) are only for manual runs.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("silver_schema", d["silver_schema"], "Silver schema")
dbutils.widgets.text("gold_schema", d["gold_schema"], "Gold schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")

ctx = get_context(dbutils)

# COMMAND ----------

from vstone_framework.config import load_config
from vstone_framework.gold import churn_checks, gold_checks
from vstone_framework.gold_schema import KEYS
from vstone_framework.ops import ensure_ops_tables, write_check_results
from vstone_framework.quality import failed

cfg = load_config()
g, T = cfg["gold"], cfg["gold"]["tables"]
gt = lambda key: f"{ctx.gold}.{T[key]}"
ensure_ops_tables(spark, ctx.ops)

fact = spark.table(gt("fact"))
current = {T[k]: spark.table(gt(k)) for k in ("dim_date_current", "dim_region_current", "dim_metric_current")}
history = {T["dim_region"]: spark.table(gt("dim_region"))}   # only Type 2 keeps history (dim_date Type 0, dim_metric Type 1)
pks = {T[k]: list(KEYS[k][1]) for k in ("fact", "dim_date_current", "dim_region_current", "dim_metric_current")}
fks = {col: T[ref] for col, (ref, _) in KEYS["fact"][2].items()}
silver_rows = spark.table(f"{ctx.silver}.{g['sources']['long']}").count()

results = gold_checks(fact, silver_rows, current, history, pks, fks, spark.table(gt("agg_top_regions")),
                      int(g["top_n"]), g["top_levels"], spark.table(gt("agg_market_trend")),
                      spark.table(gt("agg_state_sales")), cfg["levels"], T["fact"], T["dim_region_current"])
results += churn_checks(spark.table(gt("agg_churn")), spark.table(gt("agg_churn_summary")), g["churn"]["statuses"],
                        T["agg_churn"])

write_check_results(spark, ctx.ops, ctx.run_id, "gold", results)
display(spark.createDataFrame([r.__dict__ for r in results]))
bad = failed(results)
if bad:
    raise AssertionError("Gold checks failed:\n" + "\n".join(
        f"{r.table_name} {r.check_name}: expected {r.expected}, got {r.actual}" for r in bad))
print(f"All {len(results)} Gold checks passed.")

# COMMAND ----------

# MAGIC %md ## Primary and foreign keys declared in Unity Catalog

# COMMAND ----------

display(spark.sql(f"""
  SELECT tc.table_name, tc.constraint_name, tc.constraint_type
  FROM {ctx.catalog}.information_schema.table_constraints tc
  WHERE tc.table_schema = '{ctx.gold_schema}' ORDER BY tc.table_name, tc.constraint_type DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Reconciled the fact table with Silver and proved every primary key is unique and not null.
# MAGIC - Proved referential integrity: every region, metric and month in the fact exists in its dimension.
# MAGIC - Proved dim_region (Type 2) keeps exactly one current version per key. dim_date (Type 0) and dim_metric (Type 1) keep no history.
# MAGIC - Confirmed all 6 geography levels reach Gold and the aggregates are filled; results are in `ops.sanity_check_results`.
