# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03a_bronze_copy_into - Bronze: chunk 1 CSV file (+ the 2 crosswalks) with COPY INTO
# MAGIC
# MAGIC **Goal:** load the chunk 1 CSV file into its Bronze table, and the 2 crosswalk files (not chunked)
# MAGIC straight from the raw Volume, with the SQL `COPY INTO` command.
# MAGIC
# MAGIC **Which file / table:** chosen by the **task parameter `chunk_no`** (1 for this task) - the folder,
# MAGIC file and table name are looked up in `conf/vstone_config.json`. `include_reference` decides whether
# MAGIC the crosswalks are loaded too.
# MAGIC
# MAGIC **Why COPY INTO:** it is idempotent - Delta records which files were already loaded, so re-running
# MAGIC the job loads 0 new rows instead of duplicating data.
# MAGIC
# MAGIC **Per table:** (1) schema validation - CSV header must equal the contract; (2) `CREATE TABLE IF NOT EXISTS`
# MAGIC with STRING columns + audit columns, all commented from the data dictionary; (3) table comment + tags;
# MAGIC (4) `COPY INTO` adding `load_dt, source, source_file, batch_id`; (5) result in `ops.pipeline_run_log`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Job parameters (catalog, schemas, Volumes, env, run_id) and task parameters (`chunk_no`,
# MAGIC `include_reference`) arrive as widgets. Defaults from `conf/vstone_config.json` are only for manual runs.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
# Job parameters
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("landing_schema", d["landing_schema"], "Landing schema")
dbutils.widgets.text("bronze_schema", d["bronze_schema"], "Bronze schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("raw_volume", d["raw_volume"], "Raw files Volume")
dbutils.widgets.text("chunks_volume", d["chunks_volume"], "Chunks Volume")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")
# Task parameters
dbutils.widgets.text("chunk_no", d["chunk_no"], "Chunk number")
dbutils.widgets.dropdown("include_reference", d["include_reference"], ["true", "false"], "Load crosswalks")

ctx = get_context(dbutils, ["chunk_no", "include_reference"])

# COMMAND ----------

import os
from vstone_framework.bronze import (contract_columns, copy_into_sql, create_table_ddl, first_csv_header,
                                     reference_sources, require_method, source_for_chunk)
from vstone_framework.config import load_config
from vstone_framework.metadata import column_comments, load_data_dictionary
from vstone_framework.ops import ensure_ops_tables
from vstone_framework.runner import RESULTS_SCHEMA, raise_if_failed, results_rows, run_log_logger, run_per_table
from vstone_framework.schema_checks import assert_columns_match
from vstone_framework.uc import comment_table, load_level_fields, set_tags

cfg = load_config()
ensure_ops_tables(spark, ctx.ops)
level_fields = load_level_fields(ctx.metadata_path("fields_per_level"))
dictionary = load_data_dictionary(ctx.metadata_path("data_dictionary"))

sources = [require_method(source_for_chunk(ctx.int_param("chunk_no")), "copy_into")]
if ctx.bool_param("include_reference"):
    sources += reference_sources()
print("Loading:", [(s.table, s.file_name) for s in sources])

# COMMAND ----------

def load_with_copy_into(src):
    table, folder = f"{ctx.bronze}.{src.table}", src.landing_path(ctx.chunks_root, ctx.raw_path)
    cols = contract_columns(src, level_fields)
    assert_columns_match(src.entity, cols, first_csv_header(f"{folder}/{src.file_name}"))        # 1
    spark.sql(create_table_ddl(table, cols, column_comments(cols, dictionary), src.description))   # 2
    comment_table(spark, table, src.description)                                                # 3
    set_tags(spark, table, {**cfg["tags"], "layer": "bronze", "ingestion": src.method, "entity": src.entity})
    metrics = spark.sql(copy_into_sql(table, folder, cols, src.source, ctx.run_id,
                                      pattern=src.file_name)).first().asDict()                  # 4
    loaded_now = int(metrics.get("num_inserted_rows") or metrics.get("num_affected_rows") or 0)
    return loaded_now, (1 if loaded_now else 0)

results = run_per_table(sources, lambda s: f"{ctx.bronze}.{s.table}", load_with_copy_into,
                        run_log_logger(spark, ctx.ops, ctx.run_id, "bronze", sources[0].source))  # 5
display(spark.createDataFrame(results_rows(results), RESULTS_SCHEMA))
raise_if_failed(results, "COPY INTO", ctx.ops_table("run_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Validated the chunk 1 CSV header (and each crosswalk header) against the column contract.
# MAGIC - Created the Bronze tables with every column documented from the data dictionary, plus table comments and tags.
# MAGIC - Loaded the chunk 1 file and the 2 crosswalks with `COPY INTO`, adding the 4 audit columns.
# MAGIC - Re-running loads 0 rows (COPY INTO remembers loaded files); every load is logged in `ops.pipeline_run_log`.
