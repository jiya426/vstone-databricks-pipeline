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
# MAGIC **Incremental and idempotent:** batch reads have no checkpoint, so only XML files whose path is not yet in
# MAGIC the table's `source_file` column are read (`bronze.new_files`). Re-running loads 0 rows.
# MAGIC
# MAGIC **Schema validation:** explicit all-STRING schema; unexpected values go to `_rescued_data` (must be empty).

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from pyspark.sql import functions as F
from vstone_framework.audit import with_audit_columns
from vstone_framework.bronze import (RESCUED_COL, bronze_sources, contract_columns, create_table_ddl, new_files,
                                     string_schema)
from vstone_framework.config import load_config
from vstone_framework.metadata import column_comments, load_data_dictionary
from vstone_framework.ops import ensure_ops_tables
from vstone_framework.runner import RESULTS_SCHEMA, raise_if_failed, results_rows, run_log_logger, run_per_table
from vstone_framework.runtime import get_context
from vstone_framework.uc import comment_table, load_level_fields, set_tags

cfg = load_config()
ctx = get_context(dbutils)
ensure_ops_tables(spark, ctx.ops)
level_fields = load_level_fields(f"{ctx.raw_path}/fields_per_level.json")
dictionary = load_data_dictionary(f"{ctx.raw_path}/DataDictionary.csv")

# COMMAND ----------

def load_with_pyspark_xml(src):
    table = f"{ctx.bronze}.{src.table}"
    cols = contract_columns(src, level_fields)
    spark.sql(create_table_ddl(table, cols, column_comments(cols, dictionary), src.description, rescued=True))
    comment_table(spark, table, src.description)
    set_tags(spark, table, {**cfg["tags"], "layer": "bronze", "ingestion": src.method, "entity": src.entity})

    available = [f.path for f in dbutils.fs.ls(f"{ctx.chunks_root}/{src.folder}") if f.name.endswith(".xml")]
    loaded = [r.source_file for r in spark.table(table).select("source_file").distinct().collect()]
    todo = new_files(available, loaded)
    if not todo:
        return 0, 0

    before = spark.table(table).count()
    df = (spark.read.format("xml")                                    # PySpark READ
          .option("rowTag", cfg["xml"]["row_tag"])
          .option("mode", "PERMISSIVE")
          .option("rescuedDataColumn", RESCUED_COL)
          .schema(string_schema(cols))
          .load(todo)
          .select(*[F.col(f"`{c}`") for c in cols], F.col(RESCUED_COL), F.col("_metadata"))
          .transform(with_audit_columns(src.source, ctx.run_id))
          .drop("_metadata"))
    df.write.mode("append").saveAsTable(table)                         # PySpark WRITE
    return spark.table(table).count() - before, len(todo)

sources = bronze_sources(["pyspark_xml"])
results = run_per_table(sources, lambda s: f"{ctx.bronze}.{s.table}", load_with_pyspark_xml,
                        run_log_logger(spark, ctx.ops, ctx.run_id, "bronze", sources[0].source))
display(spark.createDataFrame(results_rows(results), RESULTS_SCHEMA))
raise_if_failed(results, "XML ingestion", f"{ctx.ops}.pipeline_run_log")
