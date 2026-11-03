# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03c_bronze_xml - Day 3: XML chunk 4 into Bronze with PySpark read/write
# MAGIC
# MAGIC **What:** reads `chunks/xml/<level>/` with `spark.read.format("xml")` and appends to
# MAGIC `vstone.bronze.<level>_ts_xml` with `df.write.saveAsTable` (6 tables). Native XML support -
# MAGIC no spark-xml library needed (same approach as the reference notebook).
# MAGIC
# MAGIC **Incremental and idempotent:** batch reads have no checkpoint, so the notebook compares the XML
# MAGIC files in the folder with the `source_file` values already in the table and reads only new files.
# MAGIC Re-running loads 0 rows.
# MAGIC
# MAGIC **Schema validation:** explicit all-STRING schema from `fields_per_level.json`; unexpected values
# MAGIC go to `_rescued_data` (checked to be empty by the sanity checks).

# COMMAND ----------

import os, sys, traceback
from datetime import datetime, timezone
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.audit import with_audit_columns
from vstone_framework.bronze import (RESCUED_COL, bronze_sources, contract_columns, create_table_ddl, new_files,
                                     string_schema)
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
bronze, ops, run_id = f"{catalog}.{w('bronze_schema')}", f"{catalog}.{w('ops_schema')}", w("run_id")
row_tag = cfg["xml"]["row_tag"]

ensure_ops_tables(spark, ops)
level_fields = load_level_fields(f"{raw_path}/fields_per_level.json")
dictionary = load_data_dictionary(f"{raw_path}/DataDictionary.csv")

# COMMAND ----------

def ingest_xml(src):
    table = f"{bronze}.{src.table}"
    cols = contract_columns(src, level_fields)
    spark.sql(create_table_ddl(table, cols, column_comments(cols, dictionary), src.description, rescued=True))
    comment_table(spark, table, src.description)
    set_tags(spark, table, {**cfg["tags"], "layer": "bronze", "ingestion": "pyspark_xml", "entity": src.entity})

    folder = f"{chunks_root}/{src.folder}"
    available = [f.path for f in dbutils.fs.ls(folder) if f.name.endswith(".xml")]
    loaded = [r.source_file for r in spark.table(table).select("source_file").distinct().collect()]
    todo = new_files(available, loaded)
    if not todo:
        return 0, 0

    before = spark.table(table).count()
    df = (spark.read.format("xml")                                    # PySpark READ
          .option("rowTag", row_tag)
          .option("mode", "PERMISSIVE")
          .option("rescuedDataColumn", RESCUED_COL)
          .schema(string_schema(cols))
          .load(todo)
          .select(*[F.col(f"`{c}`") for c in cols], F.col(RESCUED_COL), F.col("_metadata"))
          .transform(with_audit_columns(src.source, run_id))
          .drop("_metadata"))
    df.write.mode("append").saveAsTable(table)                         # PySpark WRITE
    return spark.table(table).count() - before, len(todo)

results, failures = [], []
for src in bronze_sources(["pyspark_xml"]):
    started = datetime.now(timezone.utc)
    table = f"{bronze}.{src.table}"
    try:
        rows, n_files = ingest_xml(src)
        log_run(spark, ops, run_id, "bronze", src.source, table, rows, n_files, "SUCCESS", started)
        results.append((table, n_files, rows, "SUCCESS", None))
        print(f"OK   {table}: {rows:,} new rows from {n_files} new file(s)")
    except Exception as e:
        failures.append(table)
        log_run(spark, ops, run_id, "bronze", src.source, table, None, None, "FAILED", started, str(e))
        results.append((table, None, None, "FAILED", str(e)[:300]))
        print(f"FAIL {table}: {e}\n{traceback.format_exc()}")

display(spark.createDataFrame(results, "table STRING, new_files INT, new_rows LONG, status STRING, error STRING"))
if failures:
    raise RuntimeError(f"XML ingestion failed for {failures}; details in {ops}.pipeline_run_log")