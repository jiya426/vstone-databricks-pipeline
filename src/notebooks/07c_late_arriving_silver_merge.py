# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 07c_late_arriving_silver_merge - MERGE INTO Silver for late-arriving data (PDF Day 7)
# MAGIC
# MAGIC **Goal:** apply the late-arriving Bronze rows to Silver **incrementally** with `MERGE INTO`
# MAGIC instead of rebuilding the whole table:
# MAGIC
# MAGIC | Case | MERGE clause |
# MAGIC |---|---|
# MAGIC | same (date, region) with a changed value (revision) | `WHEN MATCHED AND <a value changed> THEN UPDATE` |
# MAGIC | new (date, region) (next month) | `WHEN NOT MATCHED THEN INSERT` |
# MAGIC | same row arrives again (re-run) | no clause fires -> 0 rows changed (idempotent) |
# MAGIC
# MAGIC The late rows go through **the same Silver rules** as the full load (quality flags, quarantine, dedupe,
# MAGIC typing, empty-row removal, snake_case headers with the pandas UDF). The full Silver rebuild (04a) also reads
# MAGIC the late-arriving Bronze table and keeps the latest load per key, so both paths give the same result.
# MAGIC
# MAGIC **SCD Type 2 demo:** a (simulated) crosswalk correction changes one county name with a second MERGE. After
# MAGIC Silver conformed and the Gold pipeline run, `gold.dim_region` holds 2 versions of that county.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("landing_schema", d["landing_schema"], "Landing schema")
dbutils.widgets.text("raw_volume", d["raw_volume"], "Raw files Volume")
dbutils.widgets.text("bronze_schema", d["bronze_schema"], "Bronze schema")
dbutils.widgets.text("silver_schema", d["silver_schema"], "Silver schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")

ctx = get_context(dbutils)

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from vstone_framework.audit import AUDIT_COLUMNS
from vstone_framework.bronze import RESCUED_COL
from vstone_framework.chunking import LEVEL_COL
from vstone_framework.config import load_config
from vstone_framework.headers import header_mapping
from vstone_framework.incremental import merge_metrics, merge_sql, value_columns
from vstone_framework.ops import ensure_ops_tables, log_run
from vstone_framework.silver import (QUARANTINE_DDL, cast_types, dedupe_latest, drop_empty_rows, finalize_silver,
                                     flag_quality_issues, quarantine_frame, split_quarantine, union_bronze_sources)
from vstone_framework.uc import load_level_fields

cfg, scfg = load_config(), load_config()["silver"]
late = cfg["late_arriving"]
level = late["level"]
ensure_ops_tables(spark, ctx.ops)
level_fields = load_level_fields(ctx.metadata_path("fields_per_level"))
silver_table = f"{ctx.silver}.{level}_{scfg['time_series_suffix']}"
quarantine_table = f"{ctx.silver}.{scfg['quarantine_table']}"
spark.sql(QUARANTINE_DDL.format(table=quarantine_table))
started = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md ## 1. Late rows through the Silver rules

# COMMAND ----------

metrics = [c for c in level_fields[level] if c not in ("Date", "RegionName")]
contract = ["Date", "RegionName"] + metrics
late_rows = spark.table(f"{ctx.bronze}.{late['bronze_table']}").filter(F.col(LEVEL_COL) == level)
valid, bad = split_quarantine(union_bronze_sources([late_rows], contract, RESCUED_COL)
                              .transform(flag_quality_issues(metrics, scfg["region_patterns"][level], RESCUED_COL)))
bad_rows = bad.count()
if bad_rows:
    quarantine_frame(bad, level, contract, RESCUED_COL, ctx.run_id).write.mode("append").saveAsTable(quarantine_table)
mapping = header_mapping(spark, contract, method=scfg["header_udf"])                     # pandas UDF
updates = (valid.transform(dedupe_latest(["Date", "RegionName"])).transform(cast_types(metrics))
                .transform(drop_empty_rows(metrics)).transform(finalize_silver(mapping, contract, ctx.run_id)))
updates.createOrReplaceTempView("late_updates")
print(f"{late_rows.count():,} late Bronze rows -> {updates.count():,} Silver rows to merge, {bad_rows} quarantined")

# COMMAND ----------

# MAGIC %md ## 2. MERGE INTO Silver (update revisions, insert new months)

# COMMAND ----------

keys = [mapping["Date"], mapping["RegionName"]]
sql = merge_sql(silver_table, "late_updates", keys, value_columns(spark.table(silver_table).columns, AUDIT_COLUMNS))
print(sql)
spark.sql(sql)
ts = merge_metrics(spark.sql(f"DESCRIBE HISTORY {silver_table} LIMIT 1").first())
log_run(spark, ctx.ops, ctx.run_id, "silver", "merge_late_arriving", silver_table,
        ts["updated"] + ts["inserted"], None, "SUCCESS", started)
print(f"{silver_table}: version {ts['version']} {ts['operation']} -> {ts['updated']} updated, {ts['inserted']} inserted")

# COMMAND ----------

# MAGIC %md ## 3. SCD Type 2 demo: crosswalk correction with MERGE

# COMMAND ----------

demo = late["scd2_demo"]
xwalk = f"{ctx.silver}.{demo['crosswalk']}"
key, col = demo["key_column"], demo["column"]
spark.sql(f"""
  MERGE INTO {xwalk} t
  USING (SELECT '{demo['key_value']}' AS {key}) s ON t.{key} = s.{key}
  WHEN MATCHED AND NOT endswith(t.{col}, '{demo['suffix']}')
    THEN UPDATE SET t.{col} = concat(t.{col}, '{demo['suffix']}'), t.load_dt = current_timestamp(),
                    t.source = '{late['late_source']}', t.batch_id = '{ctx.run_id}'""")
xw = merge_metrics(spark.sql(f"DESCRIBE HISTORY {xwalk} LIMIT 1").first())
log_run(spark, ctx.ops, ctx.run_id, "silver", "merge_scd2_demo", xwalk, xw["updated"], None, "SUCCESS", started)
display(spark.table(xwalk).filter(F.col(key) == demo["key_value"]))
display(spark.createDataFrame([("time series", silver_table, ts["updated"], ts["inserted"]),
                               ("crosswalk correction", xwalk, xw["updated"], xw["inserted"])],
                              "change STRING, table_name STRING, rows_updated LONG, rows_inserted LONG"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Passed the late-arriving rows through the same Silver rules as the full load (quarantine, dedupe, typing, UDF headers).
# MAGIC - `MERGE INTO` Silver: revised months **updated**, the new month **inserted**, unchanged rows untouched (re-run = 0 changes).
# MAGIC - Applied a crosswalk correction with a second MERGE - the change the Gold SCD Type 2 region dimension will record.
# MAGIC - Rows changed are read from `DESCRIBE HISTORY` and logged in `ops.pipeline_run_log`.
