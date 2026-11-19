# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03c_bronze_xml - Bronze: chunk 4 XML file with PySpark read/write
# MAGIC
# MAGIC **Goal:** read the chunk 4 XML file with `spark.read.format("xml")` and append it to its Bronze table
# MAGIC (`zillow_ts_chunk4_xml`) with `df.write.saveAsTable`. Native XML support - no spark-xml library needed.
# MAGIC The chunk is chosen by the **task parameter `chunk_no`** (4 for this task).
# MAGIC
# MAGIC **Incremental and idempotent:** batch reads have no checkpoint, so the file is read only if its path is
# MAGIC not yet in the table's `source_file` column (`bronze.new_files`). Re-running loads 0 rows.
# MAGIC
# MAGIC **Schema validation:** explicit all-STRING schema; unexpected values go to `_rescued_data` (must be empty).

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
from vstone_framework.bronze import (RESCUED_COL, contract_columns, create_table_ddl, new_files, require_method,
                                     source_for_chunk, string_schema)
from vstone_framework.config import load_config
from vstone_framework.metadata import column_comments, load_data_dictionary
from vstone_framework.ops import ensure_ops_tables
from vstone_framework.runner import RESULTS_SCHEMA, raise_if_failed, results_rows, run_log_logger, run_per_table
from vstone_framework.uc import comment_table, load_level_fields, set_tags

cfg = load_config()
ensure_ops_tables(spark, ctx.ops)
level_fields = load_level_fields(ctx.metadata_path("fields_per_level"))
dictionary = load_data_dictionary(ctx.metadata_path("data_dictionary"))
src = require_method(source_for_chunk(ctx.int_param("chunk_no")), "pyspark_xml")
print("Loading:", src.table, "from", src.file_name)

# COMMAND ----------

def load_with_pyspark_xml(src):
    table = f"{ctx.bronze}.{src.table}"
    cols = contract_columns(src, level_fields)
    spark.sql(create_table_ddl(table, cols, column_comments(cols, dictionary), src.description, rescued=True))
    comment_table(spark, table, src.description)
    set_tags(spark, table, {**cfg["tags"], "layer": "bronze", "ingestion": src.method, "entity": src.entity})

    folder = src.landing_path(ctx.chunks_root, ctx.raw_path)
    available = [f.path for f in dbutils.fs.ls(folder) if f.name.endswith(f".{src.file_format}")]
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

results = run_per_table([src], lambda s: f"{ctx.bronze}.{s.table}", load_with_pyspark_xml,
                        run_log_logger(spark, ctx.ops, ctx.run_id, "bronze", src.source))
display(spark.createDataFrame(results_rows(results), RESULTS_SCHEMA))
raise_if_failed(results, "XML ingestion", ctx.ops_table("run_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Created the Bronze table for the chunk 4 XML file with documented columns, comments and tags.
# MAGIC - Read the XML file natively with PySpark (`rowTag` from config) and appended it with `saveAsTable`, adding audit columns.
# MAGIC - Only files not yet in the table are read, so a re-run loads 0 rows. The load is logged in `ops.pipeline_run_log`.
