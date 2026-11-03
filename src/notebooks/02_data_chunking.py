# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02_data_chunking - split the dataset into 4 chunks (Job: **Data Chunking**)
# MAGIC
# MAGIC | Chunk | Dates (default cut-offs) | Written as | Read later by |
# MAGIC |---|---|---|---|
# MAGIC | 1 | 1996-04 to 2005-12 | CSV `chunks/csv_initial/<level>/` | COPY INTO |
# MAGIC | 2 | 2006-01 to 2012-12 | CSV `chunks/csv_incremental/<level>/` | Delta Live Tables |
# MAGIC | 3 | 2013-01 to 2015-12 | JSON `chunks/json/<level>/` | Auto Loader |
# MAGIC | 4 | 2016-01 to 2017-12 | XML `chunks/xml/<level>/` | PySpark read/write |
# MAGIC
# MAGIC The two crosswalk files are copied unchanged into `chunks/csv_initial/<name>/` (assumption A2).
# MAGIC Metadata files are not chunked. All logic lives in `vstone_framework` (chunking, chunk_io, manifest).
# MAGIC
# MAGIC **Multiple run scenarios** (PDF: "Notebook code must handle multiple run scenarios")
# MAGIC
# MAGIC | Scenario | What happens |
# MAGIC |---|---|
# MAGIC | First run | Every chunk is written and recorded as SUCCESS in `ops.chunk_manifest` |
# MAGIC | Re-run, nothing changed | Whole file **skipped without reading it**; no new files, so Bronze never loads rows twice |
# MAGIC | Re-run after a partial failure | Only FAILED or missing chunks are written |
# MAGIC | Cut-off dates changed | Run stops with a clear error unless `force_rechunk = true` |
# MAGIC | `force_rechunk = true` | All chunks are overwritten on purpose (Bronze must then be reset) |
# MAGIC | Output folder deleted by hand | Detected (folder missing) and re-written |

# COMMAND ----------

import json, os, sys, traceback
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.chunk_io import read_back_count, write_chunk
from vstone_framework.chunking import (CHUNK_COL, CHUNK_TARGETS, REFERENCE_FILES, TIME_SERIES_FILES,
                                       chunk_output_path, invalid_xml_names, parse_cutoffs, plan_action,
                                       reconcile_counts, with_chunk_no)
from vstone_framework.config import load_config
from vstone_framework.manifest import clear_file_failure, ensure_manifest, manifest_lookup, record
from vstone_framework.runtime import get_context
from vstone_framework.schema_checks import assert_columns_match
from vstone_framework.uc import load_level_fields

cfg = load_config()
ctx = get_context(dbutils, {"chunk1_end_date": "", "chunk2_end_date": "", "chunk3_end_date": "",
                            "force_rechunk": "false"})
cutoffs = parse_cutoffs([ctx.param("chunk1_end_date"), ctx.param("chunk2_end_date"), ctx.param("chunk3_end_date")])
cutoff_key = ",".join(c.isoformat() for c in cutoffs)
force = ctx.param("force_rechunk").lower() == "true"
manifest_table = f"{ctx.ops}.chunk_manifest"
level_fields = load_level_fields(f"{ctx.raw_path}/fields_per_level.json")
print(f"run_id={ctx.run_id} cut-offs={cutoff_key} force={force}")

ensure_manifest(spark, manifest_table)
manifest = manifest_lookup(spark, manifest_table)

def path_exists(path):
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:
        return False

def log(source_file, level, chunk_no, fmt, path, rows, lo, hi, status, error=None):
    record(spark, manifest_table, source_file, level, chunk_no, fmt, path, rows, lo, hi, cutoff_key, status,
           ctx.run_id, error)

# Guard: changing cut-offs after a successful split would silently mix two splits downstream
previous = {r.cutoffs for r in manifest.values() if r.status == "SUCCESS" and r.chunk_no > 0} - {None}
if previous and previous != {cutoff_key} and not force:
    raise ValueError(f"Cut-offs changed from {previous} to {cutoff_key}. "
                     "Set force_rechunk=true to re-split (Bronze must then be reset).")

# COMMAND ----------

# MAGIC %md ## Split the six time-series files

# COMMAND ----------

failures, summary = [], []
for file_name, level in TIME_SERIES_FILES.items():
    # Fast path: all 4 chunks already SUCCESS and on disk -> skip without reading the 1.9 GB source
    prior = {n: manifest.get((file_name, n)) for n in CHUNK_TARGETS}
    if all(plan_action(p.status if p else None, path_exists(chunk_output_path(ctx.chunks_root, n, level)), force)
           == "skip" for n, p in prior.items()):
        summary += [(file_name, n, CHUNK_TARGETS[n].file_format, "skipped", p.row_count) for n, p in prior.items()]
        print(f"SKIP {file_name}: all chunks already written")
        continue
    try:
        df = spark.read.option("header", True).option("inferSchema", False).csv(f"{ctx.raw_path}/{file_name}")
        assert_columns_match(level, level_fields[level], df.columns)
        if invalid_xml_names(df.columns):
            raise ValueError(f"Columns not valid as XML element names: {invalid_xml_names(df.columns)}")

        df = df.transform(with_chunk_no(cutoffs))
        stats = {r[CHUNK_COL]: r for r in df.groupBy(CHUNK_COL).agg(
            F.count(F.lit(1)).alias("n"), F.min("Date").alias("lo"), F.max("Date").alias("hi")).collect()}
        if None in stats:
            raise ValueError(f"{stats[None].n} rows have an unparseable Date")

        written = {}
        for n, target in CHUNK_TARGETS.items():
            path = chunk_output_path(ctx.chunks_root, n, level)
            rows = stats[n].n if n in stats else 0
            p = prior[n]
            action = plan_action(p.status if p else None, path_exists(path), force)
            if action == "skip":
                written[n] = p.row_count
                summary.append((file_name, n, target.file_format, "skipped", p.row_count))
                continue
            write_chunk(df.filter(F.col(CHUNK_COL) == n).drop(CHUNK_COL), target.file_format, path, rows,
                        cfg["chunk_rows_per_file"])
            got = read_back_count(spark, target.file_format, path)
            if got != rows:
                raise AssertionError(f"{file_name} chunk {n}: wrote {rows}, read back {got}")
            written[n] = got
            log(file_name, level, n, target.file_format, path, got,
                stats[n].lo if rows else None, stats[n].hi if rows else None, "SUCCESS")
            summary.append((file_name, n, target.file_format, action, got))

        reconcile_counts(sum(r.n for r in stats.values()), written)
        clear_file_failure(spark, manifest_table, file_name)
        print(f"OK   {file_name}: {written}")
    except Exception as e:
        failures.append(file_name)
        log(file_name, level, 0, None, None, None, None, None, "FAILED", str(e))
        print(f"FAIL {file_name}: {e}\n{traceback.format_exc()}")

# COMMAND ----------

# MAGIC %md ## Copy the crosswalk (reference) files unchanged

# COMMAND ----------

for file_name, name in REFERENCE_FILES.items():
    path = chunk_output_path(ctx.chunks_root, 1, name)
    try:
        p = manifest.get((file_name, 1))
        action = plan_action(p.status if p else None, path_exists(path), force)
        if action == "skip":
            summary.append((file_name, 1, "csv", "skipped", p.row_count))
            continue
        dbutils.fs.rm(path, True)
        dbutils.fs.cp(f"{ctx.raw_path}/{file_name}", f"{path}/{file_name}")
        rows = spark.read.option("header", True).csv(path).count()
        log(file_name, name, 1, "csv", path, rows, None, None, "SUCCESS")
        summary.append((file_name, 1, "csv", action, rows))
    except Exception as e:
        failures.append(file_name)
        log(file_name, name, 1, "csv", path, None, None, None, "FAILED", str(e))
        print(f"FAIL {file_name}: {e}")

# COMMAND ----------

display(spark.createDataFrame(summary, "source_file STRING, chunk_no INT, format STRING, action STRING, rows LONG"))
if failures:
    raise RuntimeError(f"Data Chunking failed for {failures}; see {manifest_table}")   # fails the job -> email
print("Data Chunking completed.")
