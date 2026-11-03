# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03a_bronze_copy_into - Day 2: CSV chunk 1 + crosswalks into Bronze with COPY INTO
# MAGIC
# MAGIC **What:** loads `chunks/csv_initial/<level>/` (6 levels) and the 2 crosswalk files into Delta tables in
# MAGIC `vstone.bronze` with the SQL `COPY INTO` command.
# MAGIC
# MAGIC **Why COPY INTO:** it is idempotent - Delta records which files were already loaded, so re-running
# MAGIC the job loads 0 new rows instead of duplicating data.
# MAGIC
# MAGIC **Per table:** (1) schema validation - CSV header must equal the contract; (2) `CREATE TABLE IF NOT EXISTS`
# MAGIC with STRING columns + audit columns, all commented from the data dictionary; (3) table comment + tags;
# MAGIC (4) `COPY INTO` adding `load_dt, source, source_file, batch_id`; (5) result in `ops.pipeline_run_log`.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.bronze import contract_columns, copy_into_sql, copy_into_sources, create_table_ddl, first_csv_header
from vstone_framework.config import load_config
from vstone_framework.metadata import column_comments, load_data_dictionary
from vstone_framework.ops import ensure_ops_tables
from vstone_framework.runner import RESULTS_SCHEMA, raise_if_failed, results_rows, run_log_logger, run_per_table
from vstone_framework.runtime import get_context
from vstone_framework.schema_checks import assert_columns_match
from vstone_framework.uc import comment_table, load_level_fields, set_tags

cfg = load_config()
ctx = get_context(dbutils)
ensure_ops_tables(spark, ctx.ops)
level_fields = load_level_fields(f"{ctx.raw_path}/fields_per_level.json")
dictionary = load_data_dictionary(f"{ctx.raw_path}/DataDictionary.csv")

# COMMAND ----------

def load_with_copy_into(src):
    table, folder = f"{ctx.bronze}.{src.table}", f"{ctx.chunks_root}/{src.folder}"
    cols = contract_columns(src, level_fields)
    assert_columns_match(src.entity, cols, first_csv_header(folder))                       # 1
    spark.sql(create_table_ddl(table, cols, column_comments(cols, dictionary), src.description))   # 2
    comment_table(spark, table, src.description)                                           # 3
    set_tags(spark, table, {**cfg["tags"], "layer": "bronze", "ingestion": src.method, "entity": src.entity})
    metrics = spark.sql(copy_into_sql(table, folder, cols, src.source, ctx.run_id)).first().asDict()  # 4
    files = len([f for f in os.listdir(folder) if f.endswith(".csv")])
    return int(metrics.get("num_inserted_rows") or metrics.get("num_affected_rows") or 0), files

sources = copy_into_sources()
results = run_per_table(sources, lambda s: f"{ctx.bronze}.{s.table}", load_with_copy_into,
                        run_log_logger(spark, ctx.ops, ctx.run_id, "bronze", sources[0].source))      # 5
display(spark.createDataFrame(results_rows(results), RESULTS_SCHEMA))
raise_if_failed(results, "COPY INTO", f"{ctx.ops}.pipeline_run_log")
