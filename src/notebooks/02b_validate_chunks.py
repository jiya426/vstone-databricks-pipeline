# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02b_validate_chunks - sanity check after Data Chunking
# MAGIC Second task of the **Data Chunking** job; fails the job if any check fails.
# MAGIC 1. 24 time-series chunks + 2 reference copies are SUCCESS in `ops.chunk_manifest`, no FAILED rows.
# MAGIC 2. Reconciliation: per level, the sum of chunk rows equals the source rows (`expected_source_rows` in config).
# MAGIC 3. Each chunk's dates lie inside its cut-off window (no overlap, no gap).
# MAGIC 4. Every output folder holds data files of the expected format.

# COMMAND ----------

import os, sys
from datetime import date
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.chunking import CHUNK_TARGETS, REFERENCE_FILES, TIME_SERIES_FILES, parse_cutoffs
from vstone_framework.config import load_config
from vstone_framework.runtime import get_context

cfg = load_config()
ctx = get_context(dbutils, {"chunk1_end_date": "", "chunk2_end_date": "", "chunk3_end_date": ""})
c1, c2, c3 = parse_cutoffs([ctx.param("chunk1_end_date"), ctx.param("chunk2_end_date"), ctx.param("chunk3_end_date")])
windows = {1: (date(1900, 1, 1), c1), 2: (c1, c2), 3: (c2, c3), 4: (c3, date(9999, 12, 31))}
expected_rows = cfg["expected_source_rows"]

m = spark.table(f"{ctx.ops}.chunk_manifest")
display(m.orderBy("level", "chunk_no"))
rows = m.collect()
errors = []

# COMMAND ----------

# 1. statuses
failed = [r.source_file for r in rows if r.status != "SUCCESS"]
if failed:
    errors.append(f"FAILED rows in manifest: {failed}")
ts = [r for r in rows if r.source_file in TIME_SERIES_FILES and r.status == "SUCCESS"]
if len(ts) != len(TIME_SERIES_FILES) * len(CHUNK_TARGETS):
    errors.append(f"Expected {len(TIME_SERIES_FILES) * len(CHUNK_TARGETS)} time-series chunks, found {len(ts)}")
refs = [r for r in rows if r.level in REFERENCE_FILES.values() and r.status == "SUCCESS"]
if len(refs) != len(REFERENCE_FILES):
    errors.append("Reference files missing from manifest")

# 2. reconciliation
for level, n in expected_rows.items():
    total = sum(r.row_count for r in ts if r.level == level)
    if total != n:
        errors.append(f"{level}: chunks sum to {total:,}, source has {n:,}")

# 3. date windows (exclusive lower bound, inclusive upper bound)
for r in ts:
    if r.row_count:
        lo, hi = windows[r.chunk_no]
        if not (lo < date.fromisoformat(r.min_date) and date.fromisoformat(r.max_date) <= hi):
            errors.append(f"{r.level} chunk {r.chunk_no} dates {r.min_date}..{r.max_date} outside {lo}..{hi}")

# 4. output folders contain data files of the right type
for r in ts:
    ext = CHUNK_TARGETS[r.chunk_no].file_format
    files = [f.name for f in dbutils.fs.ls(r.output_path)]
    if not any(f".{ext}" in f for f in files):
        errors.append(f"{r.output_path} has no .{ext} files: {files[:5]}")

if errors:
    raise AssertionError("Chunk validation failed:\n" + "\n".join(errors))
print(f"All checks passed: {len(ts)} chunks + {len(refs)} reference files, {sum(expected_rows.values()):,} rows reconciled.")
