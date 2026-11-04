# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04a_silver_time_series - Day 4: Bronze -> Silver for the 6 time-series levels
# MAGIC
# MAGIC For each level the 4 Bronze tables (COPY INTO, DLT, Auto Loader, XML) are combined and cleaned:
# MAGIC
# MAGIC | Step | Framework function | Result |
# MAGIC |---|---|---|
# MAGIC | Union 4 sources | `silver.union_bronze_sources` | one DataFrame with lineage |
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

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from pyspark.sql import functions as F
from vstone_framework.bronze import RESCUED_COL, bronze_sources
from vstone_framework.config import load_config
from vstone_framework.headers import header_mapping, mapping_rows
from vstone_framework.metadata import column_comments, load_data_dictionary
from vstone_framework.ops import ensure_ops_tables
from vstone_framework.runner import RESULTS_SCHEMA, raise_if_failed, results_rows, run_log_logger, run_per_table
from vstone_framework.runtime import get_context
from vstone_framework.silver import (QUARANTINE_DDL, cast_types, dedupe_latest, drop_empty_rows, finalize_silver,
                                     flag_quality_issues, quarantine_frame, reconcile_silver, silver_table_ddl,
                                     split_quarantine, union_bronze_sources)
from vstone_framework.uc import comment_table, load_level_fields, set_tags

cfg, scfg = load_config(), load_config()["silver"]
ctx = get_context(dbutils)
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
level_fields = load_level_fields(f"{ctx.raw_path}/fields_per_level.json")
dictionary = load_data_dictionary(f"{ctx.raw_path}/DataDictionary.csv")
all_mappings = []

# COMMAND ----------

def build_silver(level):
    table = f"{ctx.silver}.{level}_{scfg['time_series_suffix']}"
    metrics = [c for c in level_fields[level] if c not in ("Date", "RegionName")]
    contract = ["Date", "RegionName"] + metrics
    sources = [s for s in bronze_sources() if s.entity == level]
    bronze_rows = sum(spark.table(f"{ctx.bronze}.{s.table}").count() for s in sources)

    union = union_bronze_sources([spark.table(f"{ctx.bronze}.{s.table}") for s in sources], contract, RESCUED_COL)
    valid, bad = split_quarantine(union.transform(
        flag_quality_issues(metrics, scfg["region_patterns"][level], RESCUED_COL)))

    (quarantine_frame(bad, level, contract, RESCUED_COL, ctx.run_id)
        .write.mode("overwrite").option("replaceWhere", f"level = '{level}'").saveAsTable(quarantine_table))
    quarantined = spark.table(quarantine_table).filter(F.col("level") == level).count()

    deduped = valid.transform(dedupe_latest(["Date", "RegionName"]))
    deduped_rows = deduped.count()

    mapping = header_mapping(spark, contract, method=scfg["header_udf"])           # pandas UDF
    all_mappings.extend((t, o, n, scfg["header_udf"]) for t, o, n in mapping_rows(mapping, table))
    description = (f"Silver: cleaned monthly Zillow housing metrics for {level} - union of the 4 Bronze sources, "
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

results = run_per_table(cfg["levels"], lambda l: f"{ctx.silver}.{l}_{scfg['time_series_suffix']}", build_silver,
                        run_log_logger(spark, ctx.ops, ctx.run_id, "silver", "silver_time_series"))

# COMMAND ----------

if all_mappings:
    (spark.createDataFrame(all_mappings, "table_name STRING, original_name STRING, standardized_name STRING, udf_method STRING")
          .write.insertInto(map_table, overwrite=True))
display(spark.createDataFrame(results_rows(results), RESULTS_SCHEMA))
display(spark.table(recon_table).filter(F.col("run_id") == ctx.run_id))
raise_if_failed(results, "Silver time series", f"{ctx.ops}.pipeline_run_log")
