# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03b_bronze_autoloader_json - Bronze: chunk 3 JSON file with Auto Loader
# MAGIC
# MAGIC **Goal:** stream the chunk 3 JSON file into its Bronze table (`zillow_ts_chunk3_json`).
# MAGIC The chunk is chosen by the **task parameter `chunk_no`** (3 for this task); the notebook checks that
# MAGIC the chunk is configured for Auto Loader.
# MAGIC
# MAGIC **Why Auto Loader:** it discovers new files incrementally and records progress in a checkpoint,
# MAGIC so each file is processed exactly once. `trigger(availableNow=True)` processes everything new and
# MAGIC then stops - batch-like cost, streaming guarantees (the trigger serverless supports).
# MAGIC
# MAGIC **Schema validation:** explicit all-STRING schema (the combined columns of all 6 levels). The JSON writer
# MAGIC omitted null fields; the explicit schema restores them. Anything that does not fit goes to
# MAGIC `_rescued_data`, which the sanity checks require to be empty.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Job parameters and the task parameter `chunk_no` arrive as widgets; defaults are only for manual runs.

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
# Task parameter
dbutils.widgets.text("chunk_no", d["chunk_no"], "Chunk number")

ctx = get_context(dbutils, ["chunk_no"])

# COMMAND ----------

from pyspark.sql import functions as F
from vstone_framework.audit import with_audit_columns
from vstone_framework.bronze import (RESCUED_COL, contract_columns, create_table_ddl, require_method, source_for_chunk,
                                     string_schema)
from vstone_framework.config import load_config
from vstone_framework.metadata import column_comments, load_data_dictionary, sql_string
from vstone_framework.ops import ensure_ops_tables
from vstone_framework.runner import RESULTS_SCHEMA, raise_if_failed, results_rows, run_log_logger, run_per_table
from vstone_framework.uc import comment_table, load_level_fields, set_tags

cfg = load_config()
spark.sql(f"CREATE VOLUME IF NOT EXISTS {ctx.landing}.{cfg['volumes']['checkpoints']} "
          f"COMMENT {sql_string('Auto Loader / streaming checkpoints and schema locations')}")
ensure_ops_tables(spark, ctx.ops)
level_fields = load_level_fields(ctx.metadata_path("fields_per_level"))
dictionary = load_data_dictionary(ctx.metadata_path("data_dictionary"))
src = require_method(source_for_chunk(ctx.int_param("chunk_no")), "autoloader")
print("Loading:", src.table, "from", src.file_name)

# COMMAND ----------

def load_with_autoloader(src):
    table = f"{ctx.bronze}.{src.table}"
    ckpt = f"{ctx.checkpoints_root}/{src.table}"          # one checkpoint folder per Bronze table
    cols = contract_columns(src, level_fields)
    spark.sql(create_table_ddl(table, cols, column_comments(cols, dictionary), src.description, rescued=True))
    comment_table(spark, table, src.description)
    set_tags(spark, table, {**cfg["tags"], "layer": "bronze", "ingestion": src.method, "entity": src.entity})
    before = spark.table(table).count()

    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", src.file_format)
              .option("cloudFiles.schemaLocation", f"{ckpt}/schema")
              .option("rescuedDataColumn", RESCUED_COL)
              .schema(string_schema(cols))
              .load(src.landing_path(ctx.chunks_root, ctx.raw_path))
              .select(*[F.col(f"`{c}`") for c in cols], F.col(RESCUED_COL), F.col("_metadata"))
              .transform(with_audit_columns(src.source, ctx.run_id))
              .drop("_metadata"))
    query = (stream.writeStream.option("checkpointLocation", f"{ckpt}/checkpoint")
             .trigger(availableNow=True).toTable(table))
    query.awaitTermination()
    # Row count from the table itself: streaming progress reports can contain None for empty batches
    loaded_now = spark.table(table).count() - before
    return loaded_now, (1 if loaded_now else 0)

results = run_per_table([src], lambda s: f"{ctx.bronze}.{s.table}", load_with_autoloader,
                        run_log_logger(spark, ctx.ops, ctx.run_id, "bronze", src.source))
display(spark.createDataFrame(results_rows(results), RESULTS_SCHEMA))
raise_if_failed(results, "Auto Loader", ctx.ops_table("run_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Created the Bronze table for the chunk 3 JSON file with documented columns, comments and tags.
# MAGIC - Streamed the file in with Auto Loader (`availableNow`), adding the 4 audit columns and a `_rescued_data` column.
# MAGIC - The checkpoint guarantees exactly-once: a re-run loads 0 rows. The load is logged in `ops.pipeline_run_log`.
