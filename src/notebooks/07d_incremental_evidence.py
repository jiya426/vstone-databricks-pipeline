# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 07d_incremental_evidence - prove the late-arriving batch flowed to Gold (PDF Day 7)
# MAGIC **Goal:** one place that shows, with checks, what the incremental run changed in every layer:
# MAGIC
# MAGIC | # | Evidence | Expected |
# MAGIC |---|---|---|
# MAGIC | 1 | Silver table history | the MERGE version with rows updated + inserted |
# MAGIC | 2 | Revised values in Silver and Gold | revised ZIP values = original x (1 + revision %) |
# MAGIC | 3 | New month in Gold | the next month exists in `fact` and `dim_date` for the late ZIP codes |
# MAGIC | 4 | SCD Type 2 | `gold.dim_region` holds 2 versions of the corrected county: old one closed, new one current |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("bronze_schema", d["bronze_schema"], "Bronze schema")
dbutils.widgets.text("silver_schema", d["silver_schema"], "Silver schema")
dbutils.widgets.text("gold_schema", d["gold_schema"], "Gold schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")

ctx = get_context(dbutils)

# COMMAND ----------

from pyspark.sql import functions as F
from vstone_framework.chunking import LEVEL_COL
from vstone_framework.config import load_config
from vstone_framework.gold import SCD_END, SCD_START
from vstone_framework.ops import ensure_ops_tables, write_check_results
from vstone_framework.quality import _result, failed

cfg = load_config()
late, scfg, T = cfg["late_arriving"], cfg["silver"], cfg["gold"]["tables"]
demo = late["scd2_demo"]
level = late["level"]
silver_table = f"{ctx.silver}.{level}_{scfg['time_series_suffix']}"
ensure_ops_tables(spark, ctx.ops)
results = []

# COMMAND ----------

# MAGIC %md ## 1. Silver history: the MERGE

# COMMAND ----------

hist = spark.sql(f"DESCRIBE HISTORY {silver_table}").select("version", "timestamp", "operation",
                                                             "operationMetrics.numTargetRowsUpdated",
                                                             "operationMetrics.numTargetRowsInserted")
display(hist.limit(5))
merges = hist.filter("operation = 'MERGE'").count()
results.append(_result(silver_table, "merge_applied", ">= 1 MERGE version", merges, merges >= 1))

# COMMAND ----------

# MAGIC %md ## 2-3. Late rows in Gold: revised values and the new month

# COMMAND ----------

late_bronze = spark.table(f"{ctx.bronze}.{late['bronze_table']}").filter(F.col(LEVEL_COL) == level)
metric_key = late["metric_code"]
late_keys = (late_bronze.select(F.concat(F.lit(f"{level}:"), F.col("RegionName")).alias("region_key"),
                                F.date_format(F.to_date("Date"), "yyyyMMdd").cast("int").alias("date_key"),
                                F.col(metric_key).cast("decimal(18,4)").alias("late_value")))
fact = spark.table(f"{ctx.gold}.{T['fact']}").filter(F.col("metric_key") == cfg["gold"]["home_value_metric"])
compare = late_keys.join(fact.select("region_key", "date_key", "metric_value"), ["region_key", "date_key"], "left")
display(compare.orderBy("region_key", "date_key").limit(10))
missing = compare.filter("metric_value IS NULL").count()
different = compare.filter("metric_value IS NOT NULL AND metric_value != late_value").count()
results.append(_result(T["fact"], "late_rows_reached_gold", 0, missing, missing == 0))
results.append(_result(T["fact"], "late_values_match_gold", 0, different, different == 0))
new_month = late_keys.agg(F.max("date_key")).first()[0]
in_dim = spark.table(f"{ctx.gold}.{T['dim_date_current']}").filter(F.col("date_key") == new_month).count()
results.append(_result(T["dim_date_current"], "new_month_in_date_dimension", 1, in_dim, in_dim == 1))

# COMMAND ----------

# MAGIC %md ## 4. SCD Type 2: two versions of the corrected county

# COMMAND ----------

region_key = f"county:{demo['key_value']}"
versions = (spark.table(f"{ctx.gold}.{T['dim_region']}").filter(F.col("region_key") == region_key)
                 .select("region_key", "region_name", "county_name", SCD_START, SCD_END).orderBy(SCD_START))
display(versions)
n, open_versions = versions.count(), versions.filter(F.col(SCD_END).isNull()).count()
current_name = (spark.table(f"{ctx.gold}.{T['dim_region_current']}").filter(F.col("region_key") == region_key)
                     .first()[demo["column"]])
results.append(_result(T["dim_region"], "scd2_versions_for_corrected_county", ">= 2", n, n >= 2))
results.append(_result(T["dim_region"], "scd2_exactly_one_current_version", 1, open_versions, open_versions == 1))
results.append(_result(T["dim_region_current"], "current_version_has_correction", f"ends with '{demo['suffix']}'",
                       current_name, str(current_name).endswith(demo["suffix"])))

# COMMAND ----------

write_check_results(spark, ctx.ops, ctx.run_id, "incremental", results)
display(spark.createDataFrame([r.__dict__ for r in results]))
if failed(results):
    raise AssertionError("Incremental evidence failed:\n" + "\n".join(
        f"{r.table_name} {r.check_name}: expected {r.expected}, got {r.actual}" for r in failed(results)))
print(f"All {len(results)} incremental-load checks passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Showed the Silver MERGE in the table history (rows updated and inserted).
# MAGIC - Proved every late-arriving value reached the Gold fact with the revised amount, and the new month reached `dim_date`.
# MAGIC - Proved SCD Type 2: the corrected county has an old (closed) and a new (current) version in `gold.dim_region`.
