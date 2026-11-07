# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04a_silver_time_series - Day 4: Bronze -> Silver for the 6 time-series levels
# MAGIC
# MAGIC **Goal:** one clean, typed, deduplicated Silver table per geography level.
# MAGIC The 4 Bronze tables (one per landed file: COPY INTO, DLT, Auto Loader, XML) each hold all 6 levels.
# MAGIC For each level in the **task parameter `levels`** (default `all`), the rows of that level are taken from
# MAGIC all 4 tables (`RegionLevel = level`) and cleaned:
# MAGIC
# MAGIC | Step | Framework function | Result |
# MAGIC |---|---|---|
# MAGIC | Union 4 Bronze tables, filter level | `silver.union_bronze_sources` | one DataFrame with lineage, only this level's columns |
# MAGIC | Quality rules | `silver.flag_quality_issues` | NULL_KEY, BAD_DATE, INVALID_REGION, RESCUED_DATA, NON_NUMERIC |
# MAGIC | Quarantine | `silver.split_quarantine` + `quarantine_frame` | bad rows -> `silver.quarantine_records` (raw row kept as JSON) |
# MAGIC | Types | `silver.cast_types` | Date -> DATE, metrics -> DOUBLE |
# MAGIC | Nulls | `silver.drop_empty_rows` | rows with no metric at all dropped and counted |
# MAGIC | Dedupe | `silver.dedupe_latest` | one row per (date, region_name), latest load wins |
# MAGIC | Headers | `headers.header_mapping` (**pandas UDF**) + `standardize_headers` | snake_case names |
# MAGIC | Reconciliation | `silver.reconcile_silver` | bronze = silver + quarantined + empty + duplicates |
# MAGIC
# MAGIC Writes are full refreshes of each table (`insertInto(overwrite=True)`), so re-runs give the same result
# MAGIC and every run is a new Delta version (used for time travel on Day 5).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Job parameters and the task parameter `levels` arrive as widgets; defaults are only for manual runs.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
# Job parameters
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("landing_schema", d["landing_schema"], "Landing schema")
dbutils.widgets.text("bronze_schema", d["bronze_schema"], "Bronze schema")
dbutils.widgets.text("silver_schema", d["silver_schema"], "Silver schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("raw_volume", d["raw_volume"], "Raw files Volume")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")
# Task parameter
dbutils.widgets.text("levels", d["levels"], "Levels (all or comma list)")

ctx = get_context(dbutils, ["levels"])

# COMMAND ----------

from pyspark.sql import functions as F
from vstone_framework.bronze import RESCUED_COL, time_series_sources
from vstone_framework.chunking import LEVEL_COL
from vstone_framework.config import load_config
from vstone_framework.headers import header_mapping, mapping_rows
from vstone_framework.metadata import column_comments, load_data_dictionary
from vstone_framework.ops import ensure_ops_tables
from vstone_framework.runner import RESULTS_SCHEMA, raise_if_failed, results_rows, run_log_logger, run_per_table
from vstone_framework.silver import (QUARANTINE_DDL, cast_types, dedupe_latest, drop_empty_rows, finalize_silver,
                                     flag_quality_issues, quarantine_frame, reconcile_silver, silver_table_ddl,
                                     split_quarantine, union_bronze_sources)
from vstone_framework.uc import comment_table, load_level_fields, set_tags

cfg, scfg = load_config(), load_config()["silver"]
levels = ctx.list_param("levels", cfg["levels"])
bronze_tables = [f"{ctx.bronze}.{s.table}" for s in time_series_sources()]
# Day 7: late-arriving batches (if any were loaded) are part of the source; dedupe keeps the latest load per key,
# so a full rebuild gives the same result as the incremental MERGE (07c)
late_table = f"{ctx.bronze}.{cfg['late_arriving']['bronze_table']}"
if spark.catalog.tableExists(late_table):
    bronze_tables.append(late_table)
print("Bronze sources:", bronze_tables)
ensure_ops_tables(spark, ctx.ops)
quarantine_table = f"{ctx.silver}.{scfg['quarantine_table']}"
recon_table = f"{ctx.ops}.{scfg['reconciliation_table']}"
map_table = f"{ctx.silver}.{scfg['column_map_table']}"
spark.sql(QUARANTINE_DDL.format(table=quarantine_table))
spark.sql(f"""CREATE TABLE IF NOT EXISTS {recon_table} (
  run_id STRING, level STRING, bronze_rows BIGINT, quarantined BIGINT, empty_dropped BIGINT,
  duplicates_removed BIGINT, silver_rows BIGINT, balanced BOOLEAN, detail STRING, checked_at TIMESTAMP)
  COMMENT 'Bronze -> Silver reconciliation per level: every Bronze row is accounted for'""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {map_table} (
  table_name STRING, original_name STRING, standardized_name STRING, udf_method STRING)
  COMMENT 'Header standardisation map produced by the pandas/Python UDF (lineage from source to Silver names)'""")
level_fields = load_level_fields(ctx.metadata_path("fields_per_level"))
dictionary = load_data_dictionary(ctx.metadata_path("data_dictionary"))
all_mappings = []
bronze_rows_by_level = {}

# COMMAND ----------

def build_silver(level):
    table = f"{ctx.silver}.{level}_{scfg['time_series_suffix']}"
    metrics = [c for c in level_fields[level] if c not in ("Date", "RegionName")]
    contract = ["Date", "RegionName"] + metrics
    parts = [spark.table(t).filter(F.col(LEVEL_COL) == level) for t in bronze_tables]   # this level's rows
    bronze_rows = sum(p.count() for p in parts)
    bronze_rows_by_level[level] = bronze_rows

    union = union_bronze_sources(parts, contract, RESCUED_COL)
    valid, bad = split_quarantine(union.transform(
        flag_quality_issues(metrics, scfg["region_patterns"][level], RESCUED_COL)))

    (quarantine_frame(bad, level, contract, RESCUED_COL, ctx.run_id)
        .write.mode("overwrite").option("replaceWhere", f"level = '{level}'").saveAsTable(quarantine_table))
    quarantined = spark.table(quarantine_table).filter(F.col("level") == level).count()

    deduped = valid.transform(dedupe_latest(["Date", "RegionName"]))
    deduped_rows = deduped.count()

    mapping = header_mapping(spark, contract, method=scfg["header_udf"])           # pandas UDF
    all_mappings.extend((t, o, n, scfg["header_udf"]) for t, o, n in mapping_rows(mapping, table))
    description = (f"Silver: cleaned monthly Zillow housing metrics for {level} - its rows from the 4 Bronze tables, "
                   f"typed, deduplicated, snake_case headers; invalid rows in {quarantine_table}.")
    spark.sql(silver_table_ddl(table, contract, mapping, column_comments(contract, dictionary), description))
    comment_table(spark, table, description)
    set_tags(spark, table, {**cfg["tags"], "layer": "silver", "entity": level})

    (deduped.transform(cast_types(metrics))
            .transform(drop_empty_rows(metrics))
            .transform(finalize_silver(mapping, contract, ctx.run_id))
            .write.insertInto(table, overwrite=True))
    silver_rows = spark.table(table).count()

    duplicates = bronze_rows - quarantined - deduped_rows
    empty_dropped = deduped_rows - silver_rows
    error = reconcile_silver(bronze_rows, quarantined, empty_dropped, duplicates, silver_rows)
    spark.createDataFrame([(ctx.run_id, level, bronze_rows, quarantined, empty_dropped, duplicates, silver_rows,
                            error is None, error)],
                          "run_id STRING, level STRING, bronze_rows BIGINT, quarantined BIGINT, empty_dropped BIGINT, "
                          "duplicates_removed BIGINT, silver_rows BIGINT, balanced BOOLEAN, detail STRING")          .withColumn("checked_at", F.current_timestamp()).write.mode("append").saveAsTable(recon_table)
    print(f"{level}: bronze {bronze_rows:,} -> silver {silver_rows:,} | quarantined {quarantined:,} "
          f"| empty {empty_dropped:,} | duplicates {duplicates:,}")
    if error:
        raise AssertionError(error)
    return silver_rows, None

results = run_per_table(levels, lambda l: f"{ctx.silver}.{l}_{scfg['time_series_suffix']}", build_silver,
                        run_log_logger(spark, ctx.ops, ctx.run_id, "silver", "silver_time_series"))

# COMMAND ----------

if all_mappings:
    (spark.createDataFrame(all_mappings, "table_name STRING, original_name STRING, standardized_name STRING, udf_method STRING")
          .write.insertInto(map_table, overwrite=True))
display(spark.createDataFrame(results_rows(results), RESULTS_SCHEMA))
display(spark.table(recon_table).filter(F.col("run_id") == ctx.run_id))
raise_if_failed(results, "Silver time series", ctx.ops_table("run_log"))

# COMMAND ----------

# MAGIC %md ## Every Bronze row belongs to exactly one level

# COMMAND ----------

if set(levels) == set(cfg["levels"]):
    total_bronze = sum(spark.table(t).count() for t in bronze_tables)
    per_level = sum(bronze_rows_by_level.values())
    if total_bronze != per_level:
        raise AssertionError(f"4 Bronze tables hold {total_bronze:,} rows but the 6 levels account for {per_level:,}")
    print(f"OK: all {total_bronze:,} Bronze rows are accounted for by the 6 levels")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Split the 4 Bronze tables back into the 6 geography levels and built one Silver table per level.
# MAGIC - Quarantined invalid rows (with the reason and the full raw row) instead of dropping them silently.
# MAGIC - Deduplicated, typed (DATE / DOUBLE), removed rows with no metric, and standardised headers with the pandas UDF.
# MAGIC - Reconciled every level: Bronze rows = Silver + quarantined + empty + duplicates, and all Bronze rows map to a level.
