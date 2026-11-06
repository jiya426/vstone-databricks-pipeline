# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02a_remove_old_layout - remove the objects of the first chunking design (first task of **Data Chunking**)
# MAGIC
# MAGIC **Goal:** after the trainer's review the pipeline lands **4 files** (not 4 per source file). This task
# MAGIC removes what the first design left behind, so the landing zone and Bronze hold only the new layout:
# MAGIC
# MAGIC | Removed | Why |
# MAGIC |---|---|
# MAGIC | chunk folders `csv_initial`, `csv_incremental`, `json`, `xml` (24 sub-folders) | replaced by `chunk1_csv` ... `chunk4_xml`, one file each |
# MAGIC | Bronze tables `<level>_ts_csv`, `_ts_csv_dlt`, `_ts_json`, `_ts_xml` (24 tables) | replaced by 4 tables, one per file |
# MAGIC | checkpoint folder `autoloader_json` | old Auto Loader progress for the 6 per-level JSON streams |
# MAGIC | `ops.chunk_manifest` in the old format (one row per file and chunk) | now one row per landed file |
# MAGIC
# MAGIC The list comes from `conf/vstone_config.json` -> `legacy_layout`. Every step is idempotent: once the old
# MAGIC objects are gone, this task finds nothing and finishes in seconds.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Job parameters arrive as widgets; defaults (from `conf/vstone_config.json`) are only for manual runs.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("landing_schema", d["landing_schema"], "Landing schema")
dbutils.widgets.text("bronze_schema", d["bronze_schema"], "Bronze schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("chunks_volume", d["chunks_volume"], "Chunks Volume")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")

ctx = get_context(dbutils)

# COMMAND ----------

from vstone_framework.chunking import legacy_layout

legacy = legacy_layout()
removed = []

def remove_path(path):
    try:
        dbutils.fs.ls(path)
    except Exception:
        return                                   # already gone
    dbutils.fs.rm(path, True)
    removed.append(("folder", path))

for folder in legacy["chunk_folders"]:
    remove_path(f"{ctx.chunks_root}/{folder}")
for folder in legacy["checkpoint_folders"]:
    remove_path(f"{ctx.checkpoints_root}/{folder}")

for table in legacy["bronze_tables"]:
    full = f"{ctx.bronze}.{table}"
    if spark.catalog.tableExists(full):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full}")
            removed.append(("table", full))
        except Exception as e:                    # e.g. a streaming table still owned by the DLT pipeline
            print(f"WARN: could not drop {full}: {e}")

manifest = ctx.ops_table("chunk_manifest")
if spark.catalog.tableExists(manifest) and "level_counts" not in spark.table(manifest).columns:
    spark.sql(f"DROP TABLE {manifest}")           # old format; Data Chunking recreates it
    removed.append(("table", manifest))

if removed:
    display(spark.createDataFrame(removed, "object_type STRING, name STRING"))
print(f"Removed {len(removed)} objects of the old layout." if removed else "Nothing to remove: old layout already gone.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Removed the 24 old chunk folders, the 24 old Bronze tables, the old Auto Loader checkpoints and the old-format manifest.
# MAGIC - Left the landing zone ready for exactly 4 files and Bronze ready for 4 tables.
# MAGIC - Safe to run every time: it only removes objects listed in `legacy_layout`, and does nothing once they are gone.
