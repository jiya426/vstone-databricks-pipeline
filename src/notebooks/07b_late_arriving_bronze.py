# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 07b_late_arriving_bronze - incremental load of a late-arriving batch (PDF Day 7)
# MAGIC
# MAGIC **Goal:** show that the pipeline loads only *new* data. A late-arriving batch lands as **one new file** in
# MAGIC its own Volume (`landing/late_arriving`) and Auto Loader appends **only that file** to
# MAGIC `bronze.zillow_ts_late_arriving` - files loaded before are skipped by the checkpoint.
# MAGIC
# MAGIC **The batch (simulated - the Zillow extract ends in Dec 2017; config `late_arriving`):**
# MAGIC | Rows | What | Downstream effect |
# MAGIC |---|---|---|
# MAGIC | 100 ZIP codes, latest month | home value (ZHVI) revised by +1 % | MERGE **updates** Silver |
# MAGIC | same 100 ZIP codes, next month | new month of data | MERGE **inserts** into Silver |
# MAGIC
# MAGIC Values are derived from real Bronze rows; every row carries `source = autoloader_late_csv`.
# MAGIC **Idempotent:** the batch name (task parameter `late_batch`) fixes the file name; if the file already
# MAGIC exists it is not written again, and Auto Loader never reads a file twice.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Job parameters + task parameter `late_batch` arrive as widgets; defaults are only for manual runs.

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
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")
# Task parameter
dbutils.widgets.text("late_batch", d["late_batch"], "Late-arriving batch name")

ctx = get_context(dbutils, ["late_batch"])

# COMMAND ----------

from pyspark.sql import functions as F
from vstone_framework.audit import AUDIT_COLUMNS, with_audit_columns
from vstone_framework.bronze import RESCUED_COL, create_table_ddl, source_for_chunk, string_schema
from vstone_framework.chunk_io import write_single_file
from vstone_framework.chunking import LEVEL_COL, LEVELS, combined_columns
from vstone_framework.config import load_config
from vstone_framework.incremental import late_batch_frame
from vstone_framework.metadata import column_comments, load_data_dictionary, sql_string
from vstone_framework.ops import ensure_ops_tables, log_run, write_check_results
from vstone_framework.quality import failed, run_sanity_checks
from vstone_framework.uc import comment_table, load_level_fields, set_tags
from datetime import datetime, timezone

cfg = load_config()
late = cfg["late_arriving"]
ensure_ops_tables(spark, ctx.ops)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {ctx.landing}.{late['volume']} "
          f"COMMENT {sql_string('Late-arriving data batches (Day 7 incremental load); one file per batch')}")
late_root = ctx.volume_path(late["volume"])
late_file = f"{late_root}/{late['file_prefix']}_{ctx.param('late_batch')}.csv"
level_fields = load_level_fields(ctx.metadata_path("fields_per_level"))
dictionary = load_data_dictionary(ctx.metadata_path("data_dictionary"))
columns = combined_columns(level_fields)
table = f"{ctx.bronze}.{late['bronze_table']}"
started = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md ## 1. Land the batch file (only if this batch is new)

# COMMAND ----------

def exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

if exists(late_file):
    print(f"SKIP: {late_file} already landed - batch '{ctx.param('late_batch')}' is not written twice")
else:
    source = spark.table(f"{ctx.bronze}.{source_for_chunk(late['source_chunk_no']).table}").select(*columns)
    batch = late_batch_frame(source, columns, LEVEL_COL, late["level"], late["metric_code"], int(late["region_count"]),
                             float(late["revision_pct"]), float(late["new_month_growth_pct"]))
    write_single_file(batch, "csv", f"{late_root}/{cfg['combined']['staging_folder']}/{ctx.param('late_batch')}", late_file, dbutils.fs,
                      clear_folder=False)          # keep earlier batches: each batch is its own file
    print(f"Landed {late_file}")
display(dbutils.fs.ls(late_root))

# COMMAND ----------

# MAGIC %md ## 2. Auto Loader: append only new files to Bronze

# COMMAND ----------

description = (f"Bronze: late-arriving Zillow batches (Day 7 incremental load, simulated revisions and a new month) "
               f"loaded with Auto Loader from the {late['volume']} Volume. Same columns as the 4 chunk tables.")
spark.sql(create_table_ddl(table, columns, column_comments(columns, dictionary), description, rescued=True))
comment_table(spark, table, description)
set_tags(spark, table, {**cfg["tags"], "layer": "bronze", "ingestion": "autoloader", "entity": "late_arriving"})
before = spark.table(table).count()
ckpt = f"{ctx.checkpoints_root}/{late['bronze_table']}"
(spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv").option("header", "true")
      .option("cloudFiles.schemaLocation", f"{ckpt}/schema")
      .option("rescuedDataColumn", RESCUED_COL)
      .option("pathGlobFilter", "*.csv")
      .schema(string_schema(columns))
      .load(late_root)
      .select(*[F.col(f"`{c}`") for c in columns], F.col(RESCUED_COL), F.col("_metadata"))
      .transform(with_audit_columns(late["late_source"], ctx.run_id))
      .drop("_metadata")
      .writeStream.option("checkpointLocation", f"{ckpt}/checkpoint").trigger(availableNow=True).toTable(table)
      .awaitTermination())
loaded_now = spark.table(table).count() - before
log_run(spark, ctx.ops, ctx.run_id, "bronze", late["late_source"], table, loaded_now, None, "SUCCESS", started)
print(f"Auto Loader appended {loaded_now:,} new rows (0 when the batch was loaded before)")
display(spark.table(table).groupBy("source_file", "batch_id").count())

# COMMAND ----------

# MAGIC %md ## 3. Sanity checks on the late-arriving Bronze table

# COMMAND ----------

landed_rows = spark.read.option("header", True).csv(f"{late_root}/*.csv").count()
results = run_sanity_checks(spark.table(table), table, tuple(cfg["time_series_key"]), landed_rows, columns,
                            AUDIT_COLUMNS, rescued_column=RESCUED_COL, allowed_values={LEVEL_COL: LEVELS})
write_check_results(spark, ctx.ops, ctx.run_id, "bronze", results)
display(spark.createDataFrame([r.__dict__ for r in results]))
if failed(results):
    raise AssertionError("Late-arriving Bronze checks failed: " + str([r.check_name for r in failed(results)]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Landed the late-arriving batch as one new CSV file in its own Volume (skipped if the batch already exists).
# MAGIC - Loaded **only the new file** into `bronze.zillow_ts_late_arriving` with Auto Loader (checkpoint = exactly once).
# MAGIC - Checked the table: rows = landed rows, unique keys, audit columns, no rescued data, valid levels.
