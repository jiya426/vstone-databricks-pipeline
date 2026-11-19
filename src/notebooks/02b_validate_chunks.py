# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02b_validate_chunks - sanity check after Data Chunking
# MAGIC **Goal:** prove the landing zone holds exactly the 4 expected files and that no row was lost.
# MAGIC Second task of the **Data Chunking** job; fails the job (and sends the failure email) if any check fails.
# MAGIC
# MAGIC | # | Check |
# MAGIC |---|---|
# MAGIC | 1 | 4 chunks are SUCCESS in `ops.chunk_manifest` and there is no FAILED row |
# MAGIC | 2 | The chunks Volume holds exactly the 4 chunk folders, each with exactly 1 file of the right format |
# MAGIC | 3 | Reconciliation: per level, rows across the 4 files = rows of the source file (`expected_source_rows`) |
# MAGIC | 4 | Each file's dates lie inside its cut-off window (no overlap, no gap) |

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
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("chunks_volume", d["chunks_volume"], "Chunks Volume")
dbutils.widgets.text("chunk1_end_date", d["chunk1_end_date"], "Chunk 1 ends on")
dbutils.widgets.text("chunk2_end_date", d["chunk2_end_date"], "Chunk 2 ends on")
dbutils.widgets.text("chunk3_end_date", d["chunk3_end_date"], "Chunk 3 ends on")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")

ctx = get_context(dbutils, ["chunk1_end_date", "chunk2_end_date", "chunk3_end_date"])

# COMMAND ----------

import json
from datetime import date
from vstone_framework.chunking import CHUNK_TARGETS, chunk_folder, parse_cutoffs, reconcile_levels
from vstone_framework.config import load_config

cfg = load_config()
c1, c2, c3 = parse_cutoffs([ctx.param("chunk1_end_date"), ctx.param("chunk2_end_date"), ctx.param("chunk3_end_date")])
windows = {1: (date.min, c1), 2: (c1, c2), 3: (c2, c3), 4: (c3, date.max)}
expected_rows = cfg["expected_source_rows"]

m = spark.table(ctx.ops_table("chunk_manifest"))
display(m.orderBy("chunk_no"))
rows = {r.chunk_no: r for r in m.collect()}
errors = []

# 1. statuses
if any(r.status != "SUCCESS" for r in rows.values()):
    errors.append(f"FAILED rows in manifest: {[n for n, r in rows.items() if r.status != 'SUCCESS']}")
if sorted(n for n, r in rows.items() if r.status == "SUCCESS") != sorted(CHUNK_TARGETS):
    errors.append(f"Expected SUCCESS for chunks {sorted(CHUNK_TARGETS)}, manifest has {sorted(rows)}")

# 2. exactly 4 folders, exactly 1 file each, right extension
expected_folders = {chunk_folder(ctx.chunks_root, n).rsplit("/", 1)[1] for n in CHUNK_TARGETS}
actual_folders = {f.name.rstrip("/") for f in dbutils.fs.ls(ctx.chunks_root)}
if actual_folders != expected_folders:
    errors.append(f"Chunks Volume should hold only {sorted(expected_folders)}, found {sorted(actual_folders)}")
landed = []
for n, t in CHUNK_TARGETS.items():
    try:
        files = [f.name for f in dbutils.fs.ls(chunk_folder(ctx.chunks_root, n))]
    except Exception as e:
        files = []
        errors.append(f"chunk {n}: folder missing ({e})")
    landed += files
    if files != [t.file_name]:
        errors.append(f"chunk {n}: expected exactly [{t.file_name}], found {files}")
    if not t.file_name.endswith(f".{t.file_format}"):
        errors.append(f"chunk {n}: {t.file_name} is not a .{t.file_format} file")

# 3. reconciliation per source file (level)
errors += reconcile_levels(expected_rows, {n: json.loads(r.level_counts or "{}") for n, r in rows.items() if n > 0})
total = sum(r.row_count or 0 for n, r in rows.items() if n > 0)
if total != sum(expected_rows.values()):
    errors.append(f"4 files hold {total:,} rows, source has {sum(expected_rows.values()):,}")

# 4. date windows (exclusive lower bound, inclusive upper bound)
for n, r in rows.items():
    if n > 0 and r.row_count:
        lo, hi = windows[n]
        if not (lo < date.fromisoformat(r.min_date) and date.fromisoformat(r.max_date) <= hi):
            errors.append(f"chunk {n} dates {r.min_date}..{r.max_date} outside {lo}..{hi}")

if errors:
    raise AssertionError("Chunk validation failed:\n" + "\n".join(errors))
print(f"All checks passed: {len(landed)} files landed {landed}, {total:,} rows reconciled with the 6 source files.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Confirmed the landing zone holds **exactly 4 files** (2 CSV, 1 JSON, 1 XML), one per chunk folder.
# MAGIC - Reconciled the rows of every source file (level) across the 4 files: nothing lost, nothing duplicated.
# MAGIC - Confirmed every file only holds dates from its own cut-off window.
# MAGIC - Any failure stops the job before Bronze runs, and the job sends the failure email.
