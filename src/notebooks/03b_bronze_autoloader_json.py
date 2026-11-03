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
# MAGIC **Schema validation:** an explicit all-STRING schema built from `fields_per_level.json` is enforced.
# MAGIC The JSON writer omitted null fields, so inference could miss columns; the explicit schema restores them.
# MAGIC Any value that does not fit the schema is kept in `_rescued_data` instead of being lost,
# MAGIC and the sanity checks require `_rescued_data` to be empty.

# COMMAND ----------

import os, sys, traceback
from datetime import datetime, timezone
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.audit import with_audit_columns
from vstone_framework.bronze import RESCUED_COL, bronze_sources, contract_columns, create_table_ddl, string_schema
from vstone_framework.config import load_config
from vstone_framework.metadata import column_comments, load_data_dictionary
from vstone_framework.ops import ensure_ops_tables, log_run
from vstone_framework.uc import comment_table, load_level_fields, set_tags

cfg = load_config()
for name, default in [("catalog", "vstone"), ("landing_schema", "landing"), ("raw_volume", "raw"),
                      ("chunks_volume", "chunks"), ("bronze_schema", "bronze"), ("ops_schema", "ops"),
                      ("run_id", "manual")]:
    dbutils.widgets.text(name, default)
w = dbutils.widgets.get
catalog, landing = w("catalog"), w("landing_schema")
raw_path = f"/Volumes/{catalog}/{landing}/{w('raw_volume')}"
chunks_root = f"/Volumes/{catalog}/{landing}/{w('chunks_volume')}"
ckpt_root = f"/Volumes/{catalog}/{landing}/{cfg['volumes']['checkpoints']}"
bronze, ops, run_id = f"{catalog}.{w('bronze_schema')}", f"{catalog}.{w('ops_schema')}", w("run_id")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{landing}.{cfg['volumes']['checkpoints']} "
          "COMMENT 'Auto Loader / streaming checkpoints and schema locations'")
ensure_ops_tables(spark, ops)
level_fields = load_level_fields(f"{raw_path}/fields_per_level.json")
dictionary = load_data_dictionary(f"{raw_path}/DataDictionary.csv")

# COMMAND ----------

def ingest_json(src):
    table = f"{bronze}.{src.table}"
    cols = contract_columns(src, level_fields)
    spark.sql(create_table_ddl(table, cols, column_comments(cols, dictionary), src.description, rescued=True))
    comment_table(spark, table, src.description)
    set_tags(spark, table, {**cfg["tags"], "layer": "bronze", "ingestion": "autoloader", "entity": src.entity})
    before = spark.table(table).count()                                    # <-- NEW

    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", f"{ckpt_root}/autoloader_json/{src.entity}/schema")
              .option("rescuedDataColumn", RESCUED_COL)
              .schema(string_schema(cols))
              .load(f"{chunks_root}/{src.folder}")
              .select(*[F.col(f"`{c}`") for c in cols], F.col(RESCUED_COL), F.col("_metadata"))
              .transform(with_audit_columns(src.source, run_id))
              .drop("_metadata"))
    query = (stream.writeStream
             .option("checkpointLocation", f"{ckpt_root}/autoloader_json/{src.entity}/checkpoint")
             .trigger(availableNow=True)
             .toTable(table))
    query.awaitTermination()
    # Row count from the table itself: streaming progress reports can contain None for empty batches
    return spark.table(table).count() - before                             # <-- NEW (replaces the sum(...) line)

results, failures = [], []
for src in bronze_sources(["autoloader"]):
    started = datetime.now(timezone.utc)
    table = f"{bronze}.{src.table}"
    try:
        rows = ingest_json(src)
        log_run(spark, ops, run_id, "bronze", src.source, table, rows, None, "SUCCESS", started)
        results.append((table, rows, "SUCCESS", None))
        print(f"OK   {table}: {rows:,} new rows")
    except Exception as e:
        failures.append(table)
        log_run(spark, ops, run_id, "bronze", src.source, table, None, None, "FAILED", started, str(e))
        results.append((table, None, "FAILED", str(e)[:300]))
        print(f"FAIL {table}: {e}\n{traceback.format_exc()}")

display(spark.createDataFrame(results, "table STRING, new_rows LONG, status STRING, error STRING"))
if failures:
    raise RuntimeError(f"Auto Loader failed for {failures}; details in {ops}.pipeline_run_log")