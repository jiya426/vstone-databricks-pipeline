# Databricks notebook source
# MAGIC %md
# MAGIC # 02b_validate_chunks - sanity check after Data Chunking
# MAGIC Runs as the second task of the **Data Chunking** job. It fails the job if any check fails.
# MAGIC 1. 24 time-series chunks + 2 reference copies are SUCCESS in `ops.chunk_manifest`, no FAILED rows.
# MAGIC 2. Per level, the sum of chunk rows equals the source row count from profiling.
# MAGIC 3. Each chunk's dates lie inside its cut-off window (no overlap, no gap).
# MAGIC 4. Every output folder exists and holds data files of the expected format.

# COMMAND ----------

import os, sys
from datetime import date
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.chunking import CHUNK_TARGETS, TIME_SERIES_FILES, parse_cutoffs

dbutils.widgets.text("catalog", "vstone")
dbutils.widgets.text("ops_schema", "ops")
dbutils.widgets.text("chunk1_end_date", "2005-12-31")
dbutils.widgets.text("chunk2_end_date", "2012-12-31")
dbutils.widgets.text("chunk3_end_date", "2015-12-31")
w = dbutils.widgets.get
manifest_table = f"{w('catalog')}.{w('ops_schema')}.chunk_manifest"
c1, c2, c3 = parse_cutoffs([w("chunk1_end_date"), w("chunk2_end_date"), w("chunk3_end_date")])
windows = {1: (date(1900, 1, 1), c1), 2: (c1, c2), 3: (c2, c3), 4: (c3, date(9999, 12, 31))}

expected_rows = {"state": 13212, "metro": 211182, "county": 518791, "city": 3762566,
                 "neighborhood": 1695038, "zip": 4383885}

m = spark.table(manifest_table)
display(m.orderBy("level", "chunk_no"))
rows = m.collect()
errors = []

# 1. statuses
failed = [r.source_file for r in rows if r.status != "SUCCESS"]
if failed:
    errors.append(f"FAILED rows in manifest: {failed}")
ts = [r for r in rows if r.source_file in TIME_SERIES_FILES and r.status == "SUCCESS"]
if len(ts) != 24:
    errors.append(f"Expected 24 time-series chunks, found {len(ts)}")
if len([r for r in rows if r.level in ("county_crosswalk", "city_crosswalk") and r.status == "SUCCESS"]) != 2:
    errors.append("Reference files missing from manifest")

# 2. reconciliation
for level, n in expected_rows.items():
    total = sum(r.row_count for r in ts if r.level == level)
    if total != n:
        errors.append(f"{level}: chunks sum to {total:,}, source has {n:,}")

# 3. date windows (exclusive lower bound, inclusive upper bound)
for r in ts:
    if r.row_count == 0:
        continue
    lo, hi = windows[r.chunk_no]
    if not (lo < date.fromisoformat(r.min_date) and date.fromisoformat(r.max_date) <= hi):
        errors.append(f"{r.level} chunk {r.chunk_no} dates {r.min_date}..{r.max_date} outside window {lo}..{hi}")

# 4. output folders contain data files of the right type
for r in ts:
    ext = CHUNK_TARGETS[r.chunk_no].file_format
    files = [f.name for f in dbutils.fs.ls(r.output_path)]
    if not any(f.endswith(f".{ext}") or f".{ext}" in f for f in files):
        errors.append(f"{r.output_path} has no .{ext} files: {files[:5]}")

if errors:
    raise AssertionError("Chunk validation failed:\n" + "\n".join(errors))
print(f"All checks passed: 24 chunks + 2 reference files, {sum(expected_rows.values()):,} rows reconciled.")