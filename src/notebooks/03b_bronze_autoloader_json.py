# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03b_bronze_autoloader_json - Day 3: JSON chunk 3 into Bronze with Auto Loader
# MAGIC
# MAGIC **What:** streams `chunks/json/<level>/` into `vstone.bronze.<level>_ts_json` (6 tables).
# MAGIC
# MAGIC **Why Auto Loader:** it discovers new files incrementally and records progress in a checkpoint,
# MAGIC so each file is processed exactly once. `trigger(availableNow=True)` processes everything new and
# MAGIC then stops - batch-like cost, streaming guarantees (the trigger serverless supports).
# MAGIC
# MAGIC **Schema validation:** explicit all-STRING schema from `fields_per_level.json` (JSON matches fields by
# MAGIC name; the writer omitted null fields, which the explicit schema restores). Anything that does not fit
# MAGIC goes to `_rescued_data`, which the sanity checks require to be empty.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from pyspark.sql import functions as F
from vstone_framework.audit import with_audit_columns
from vstone_framework.bronze import RESCUED_COL, bronze_sources, contract_columns, create_table_ddl, string_schema
from vstone_framework.config import load_config
from vstone_framework.metadata import column_comments, load_data_dictionary, sql_string
from vstone_framework.ops import ensure_ops_tables
from vstone_framework.runner import RESULTS_SCHEMA, raise_if_failed, results_rows, run_log_logger, run_per_table
from vstone_framework.runtime import get_context
from vstone_framework.uc import comment_table, load_level_fields, set_tags

cfg = load_config()
ctx = get_context(dbutils)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {ctx.landing}.{cfg['volumes']['checkpoints']} "
          f"COMMENT {sql_string('Auto Loader / streaming checkpoints and schema locations')}")
ensure_ops_tables(spark, ctx.ops)
level_fields = load_level_fields(f"{ctx.raw_path}/fields_per_level.json")
dictionary = load_data_dictionary(f"{ctx.raw_path}/DataDictionary.csv")

# COMMAND ----------

def load_with_autoloader(src):
    table = f"{ctx.bronze}.{src.table}"
    ckpt = f"{ctx.checkpoints_root}/autoloader_json/{src.entity}"
    cols = contract_columns(src, level_fields)
    spark.sql(create_table_ddl(table, cols, column_comments(cols, dictionary), src.description, rescued=True))
    comment_table(spark, table, src.description)
    set_tags(spark, table, {**cfg["tags"], "layer": "bronze", "ingestion": src.method, "entity": src.entity})
    before = spark.table(table).count()

    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", f"{ckpt}/schema")
              .option("rescuedDataColumn", RESCUED_COL)
              .schema(string_schema(cols))
              .load(f"{ctx.chunks_root}/{src.folder}")
              .select(*[F.col(f"`{c}`") for c in cols], F.col(RESCUED_COL), F.col("_metadata"))
              .transform(with_audit_columns(src.source, ctx.run_id))
              .drop("_metadata"))
    query = (stream.writeStream.option("checkpointLocation", f"{ckpt}/checkpoint")
             .trigger(availableNow=True).toTable(table))
    query.awaitTermination()
    # Row count from the table itself: streaming progress reports can contain None for empty batches
    return spark.table(table).count() - before, None

sources = bronze_sources(["autoloader"])
results = run_per_table(sources, lambda s: f"{ctx.bronze}.{s.table}", load_with_autoloader,
                        run_log_logger(spark, ctx.ops, ctx.run_id, "bronze", sources[0].source))
display(spark.createDataFrame(results_rows(results), RESULTS_SCHEMA))
raise_if_failed(results, "Auto Loader", f"{ctx.ops}.pipeline_run_log")
