# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03z_bronze_sanity_checks - sanity checks after Bronze ingestion
# MAGIC **Goal:** prove every Bronze table holds exactly what was landed, once, with valid keys and audit columns.
# MAGIC Runs after the 4 ingestion tasks. Which tables are checked comes from the **task parameters**
# MAGIC `chunk_nos` (default `1,2,3,4`) and `include_reference` (the 2 crosswalks).
# MAGIC
# MAGIC | Check | Rule |
# MAGIC |---|---|
# MAGIC | row_count_matches_source | rows in the table = rows in its landed file (`ops.chunk_manifest`; crosswalks: profiled counts in config) |
# MAGIC | no_null_keys | key columns (Date, RegionName, RegionLevel / FIPS / Unique_City_ID) never null |
# MAGIC | no_duplicate_keys | each key appears once - proves a re-run did not load the file twice |
# MAGIC | audit_columns_populated | load_dt, source, source_file, batch_id never null |
# MAGIC | no_rescued_data | JSON / XML / DLT tables: nothing fell outside the expected schema |
# MAGIC | RegionLevel_in_allowed_values | time-series tables: every row belongs to one of the 6 configured levels |
# MAGIC | columns_match_contract | exactly the contracted columns (+ _rescued_data) + audit columns |
# MAGIC
# MAGIC Results go to `ops.sanity_check_results`; any FAIL fails the task (and the job emails you).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

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
# Task parameters
dbutils.widgets.text("chunk_nos", d["chunk_nos"], "Chunks to check")
dbutils.widgets.dropdown("include_reference", d["include_reference"], ["true", "false"], "Check crosswalks")

ctx = get_context(dbutils, ["chunk_nos", "include_reference"])

# COMMAND ----------

from vstone_framework.audit import AUDIT_COLUMNS
from vstone_framework.bronze import RESCUED_COL, contract_columns, reference_sources, time_series_sources
from vstone_framework.chunking import CHUNK_TARGETS, LEVEL_COL, LEVELS
from vstone_framework.config import load_config
from vstone_framework.ops import ensure_ops_tables, write_check_results
from vstone_framework.quality import failed, run_sanity_checks
from vstone_framework.uc import load_level_fields

cfg = load_config()
ensure_ops_tables(spark, ctx.ops)
level_fields = load_level_fields(ctx.metadata_path("fields_per_level"))
chunk_nos = [int(n) for n in ctx.list_param("chunk_nos", [str(n) for n in CHUNK_TARGETS])]
landed_rows = {r.chunk_no: r.row_count
               for r in spark.table(ctx.ops_table("chunk_manifest")).filter("status = 'SUCCESS'").collect()}

sources = time_series_sources(chunk_nos) + (reference_sources() if ctx.bool_param("include_reference") else [])

# COMMAND ----------

all_results = []
for src in sources:
    table = f"{ctx.bronze}.{src.table}"
    expected = cfg["expected_reference_rows"][src.entity] if src.is_reference else landed_rows.get(src.chunk_no)
    all_results += run_sanity_checks(
        spark.table(table), table, src.key_columns, expected, contract_columns(src, level_fields), AUDIT_COLUMNS,
        rescued_column=RESCUED_COL if src.has_rescued else None,
        allowed_values=None if src.is_reference else {LEVEL_COL: LEVELS})

write_check_results(spark, ctx.ops, ctx.run_id, "bronze", all_results)
display(spark.createDataFrame([r.__dict__ for r in all_results]))

bad = failed(all_results)
if bad:
    raise AssertionError("Sanity checks failed:\n" + "\n".join(
        f"{r.table_name} {r.check_name}: expected {r.expected}, got {r.actual}" for r in bad))
print(f"All {len(all_results)} sanity checks passed across {len(sources)} Bronze tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Reconciled every Bronze table with the file it was loaded from (row counts from `ops.chunk_manifest`).
# MAGIC - Proved keys are complete and unique, audit columns are filled, no data fell into `_rescued_data`,
# MAGIC   every row has a valid `RegionLevel`, and the columns match the contract.
# MAGIC - Stored every result in `ops.sanity_check_results`; a single FAIL stops the job and triggers the failure email.
