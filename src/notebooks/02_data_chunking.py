# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02_data_chunking - land the raw dataset as exactly 4 files (Job: **Data Chunking**)
# MAGIC
# MAGIC **Goal:** turn the raw Zillow dataset into the **4 files** the rest of the pipeline ingests:
# MAGIC 2 CSV, 1 JSON and 1 XML. Each file becomes one Bronze table.
# MAGIC
# MAGIC **How:**
# MAGIC 1. Read the 6 time-series files (state, metro, county, city, neighborhood, zip) and validate each header.
# MAGIC 2. **Combine** them into one dataset: union by column name, add `RegionLevel` (which file a row came from);
# MAGIC    metrics a level does not report are NULL.
# MAGIC 3. Split the combined dataset by `Date` into 4 chunks (cut-off dates are job parameters).
# MAGIC 4. Write each chunk as **one file** in its own folder, read it back and reconcile the row counts.
# MAGIC
# MAGIC | Chunk | Dates (default cut-offs) | Landed file | Bronze method |
# MAGIC |---|---|---|---|
# MAGIC | 1 | 1996-04 to 2005-12 | `chunk1_csv/zillow_time_series_chunk1.csv` | COPY INTO |
# MAGIC | 2 | 2006-01 to 2012-12 | `chunk2_csv/zillow_time_series_chunk2.csv` | Delta Live Tables |
# MAGIC | 3 | 2013-01 to 2015-12 | `chunk3_json/zillow_time_series_chunk3.json` | Auto Loader |
# MAGIC | 4 | 2016-01 to 2017-12 | `chunk4_xml/zillow_time_series_chunk4.xml` | PySpark read/write |
# MAGIC
# MAGIC Folder names, file names and formats come from `conf/vstone_config.json` -> `chunks`.
# MAGIC The 2 crosswalk files are not chunked (Bronze loads them straight from the raw Volume) and the
# MAGIC 3 metadata files are never ingested.
# MAGIC
# MAGIC **Multiple run scenarios** (PDF: "Notebook code must handle multiple run scenarios")
# MAGIC
# MAGIC | Scenario | What happens |
# MAGIC |---|---|
# MAGIC | First run | All 4 files are written and recorded as SUCCESS in `ops.chunk_manifest` |
# MAGIC | Re-run, nothing changed | **Skipped without reading the source**; the files stay unchanged, so Bronze never loads rows twice |
# MAGIC | Re-run after a partial failure | Only FAILED or missing chunks are written |
# MAGIC | Cut-off dates changed | Run stops with a clear error unless `force_rechunk = true` |
# MAGIC | `force_rechunk = true` | All 4 files are overwritten on purpose (Bronze must then be reset) |
# MAGIC | A landed file deleted by hand | Detected (file missing) and re-written |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Widgets receive the **job parameters** (same for every task of the job, from `databricks.yml` variables)
# MAGIC and the **task parameters** (only this task). The defaults come from `conf/vstone_config.json` and are used
# MAGIC only when the notebook is run by hand; in a job run the job/task values override them.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
# Job parameters
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("landing_schema", d["landing_schema"], "Landing schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("raw_volume", d["raw_volume"], "Raw files Volume")
dbutils.widgets.text("chunks_volume", d["chunks_volume"], "Chunks Volume")
dbutils.widgets.text("chunk1_end_date", d["chunk1_end_date"], "Chunk 1 ends on")
dbutils.widgets.text("chunk2_end_date", d["chunk2_end_date"], "Chunk 2 ends on")
dbutils.widgets.text("chunk3_end_date", d["chunk3_end_date"], "Chunk 3 ends on")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")
# Task parameter
dbutils.widgets.dropdown("force_rechunk", d["force_rechunk"], ["false", "true"], "Force re-chunk")

ctx = get_context(dbutils, ["chunk1_end_date", "chunk2_end_date", "chunk3_end_date", "force_rechunk"])
print(ctx)

# COMMAND ----------

import traceback
from pyspark.sql import functions as F
from vstone_framework.chunk_io import read_back_count, write_single_file
from vstone_framework.chunking import (CHUNK_COL, CHUNK_TARGETS, COMBINED_ENTITY, LEVEL_COL, TIME_SERIES_FILES,
                                       chunk_file, combine_levels, combined_columns, invalid_xml_names,
                                       parse_cutoffs, plan_action, reconcile_levels, staging_folder, with_chunk_no)
from vstone_framework.config import load_config
from vstone_framework.manifest import clear_run_failure, ensure_manifest, level_counts_json, manifest_lookup, record
from vstone_framework.schema_checks import assert_columns_match
from vstone_framework.uc import load_level_fields

cfg = load_config()
cutoffs = parse_cutoffs([ctx.param("chunk1_end_date"), ctx.param("chunk2_end_date"), ctx.param("chunk3_end_date")])
cutoff_key = ",".join(c.isoformat() for c in cutoffs)
force = ctx.bool_param("force_rechunk")
manifest_table = ctx.ops_table("chunk_manifest")
level_fields = load_level_fields(ctx.metadata_path("fields_per_level"))
columns = combined_columns(level_fields)
print(f"env={ctx.env} run_id={ctx.run_id} cut-offs={cutoff_key} force={force} combined columns={len(columns)}")

ensure_manifest(spark, manifest_table)
manifest = manifest_lookup(spark, manifest_table)

def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

# Guard: changing cut-offs after a successful split would silently mix two splits downstream
previous = {r.cutoffs for r in manifest.values() if r.status == "SUCCESS" and r.chunk_no > 0} - {None}
if previous and previous != {cutoff_key} and not force:
    raise ValueError(f"Cut-offs changed from {previous} to {cutoff_key}. "
                     "Set force_rechunk=true to re-split (Bronze must then be reset).")

plan = {}
for n in CHUNK_TARGETS:
    prior = manifest.get(n)
    plan[n] = plan_action(prior.status if prior else None, file_exists(chunk_file(ctx.chunks_root, n)), force)
print("Plan:", plan)

# COMMAND ----------

# MAGIC %md ## Combine the 6 time-series files and split them into 4 files

# COMMAND ----------

summary = []
if all(action == "skip" for action in plan.values()):
    # Fast path: all 4 files already landed and verified -> do not even read the 1.9 GB source
    summary = [(n, CHUNK_TARGETS[n].file_name, CHUNK_TARGETS[n].file_format, "skipped", manifest[n].row_count)
               for n in CHUNK_TARGETS]
    print("SKIP: all 4 chunk files already landed")
else:
    try:
        frames = {}
        for file_name, level in TIME_SERIES_FILES.items():
            df = spark.read.option("header", True).option("inferSchema", False).csv(f"{ctx.raw_path}/{file_name}")
            assert_columns_match(level, level_fields[level], df.columns)          # schema validation per file
            frames[level] = df
        if invalid_xml_names(columns):
            raise ValueError(f"Columns not valid as XML element names: {invalid_xml_names(columns)}")

        combined = combine_levels(frames, columns).transform(with_chunk_no(cutoffs))
        stats = combined.groupBy(CHUNK_COL, LEVEL_COL).agg(
            F.count(F.lit(1)).alias("n"), F.min("Date").alias("lo"), F.max("Date").alias("hi")).collect()
        if any(r[CHUNK_COL] is None for r in stats):
            raise ValueError(f"{sum(r.n for r in stats if r[CHUNK_COL] is None)} rows have an unparseable Date")

        per_chunk = {n: {"rows": 0, "levels": {}, "lo": None, "hi": None} for n in CHUNK_TARGETS}
        for r in stats:
            c = per_chunk[r[CHUNK_COL]]
            c["rows"] += r.n
            c["levels"][r[LEVEL_COL]] = r.n
            c["lo"] = min(filter(None, [c["lo"], r.lo]))
            c["hi"] = max(filter(None, [c["hi"], r.hi]))
        errors = reconcile_levels(cfg["expected_source_rows"], {n: c["levels"] for n, c in per_chunk.items()})
        if errors:                                         # source -> combined reconciliation, per source file
            raise AssertionError("Combined dataset does not match the source files:\n" + "\n".join(errors))

        for n, target in CHUNK_TARGETS.items():
            path, c = chunk_file(ctx.chunks_root, n), per_chunk[n]
            if plan[n] == "skip":
                summary.append((n, target.file_name, target.file_format, "skipped", manifest[n].row_count))
                continue
            write_single_file(combined.filter(F.col(CHUNK_COL) == n).drop(CHUNK_COL), target.file_format,
                              staging_folder(ctx.chunks_root, n), path, dbutils.fs)
            got = read_back_count(spark, target.file_format, path)
            if got != c["rows"]:
                raise AssertionError(f"chunk {n}: wrote {c['rows']}, read back {got}")
            record(spark, manifest_table, n, target.file_name, COMBINED_ENTITY, target.file_format, path, got,
                   level_counts_json(c["levels"]), c["lo"], c["hi"], cutoff_key, "SUCCESS", ctx.run_id)
            summary.append((n, target.file_name, target.file_format, plan[n], got))
            print(f"OK   chunk {n}: {got:,} rows -> {path}")
        clear_run_failure(spark, manifest_table)
    except Exception as e:
        record(spark, manifest_table, 0, None, COMBINED_ENTITY, None, None, None, None, None, None, cutoff_key,
               "FAILED", ctx.run_id, str(e))
        print(traceback.format_exc())
        raise                                              # fails the job -> failure email

# COMMAND ----------

display(spark.createDataFrame(summary, "chunk_no INT, file_name STRING, format STRING, action STRING, rows LONG"))
display(spark.table(manifest_table).orderBy("chunk_no"))
print(f"Data Chunking completed: {sum(r[4] for r in summary):,} rows in {len(summary)} files.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Read and schema-validated the 6 raw time-series files.
# MAGIC - Combined them into one dataset with a `RegionLevel` column (no data lost: rows per level reconcile with the source).
# MAGIC - Landed **exactly 4 files** in the chunks Volume: 2 CSV, 1 JSON, 1 XML - one file per folder.
# MAGIC - Recorded each file's rows, rows per level, date range and status in `ops.chunk_manifest`, which makes re-runs safe.
# MAGIC - Every name, path, date and format came from job/task parameters or the config file - nothing hard-coded.
