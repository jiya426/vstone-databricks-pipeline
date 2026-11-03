# Databricks notebook source
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
# MAGIC Metadata files are not chunked.
# MAGIC
# MAGIC **Multiple run scenarios** (PDF: "Notebook code must handle multiple run scenarios")
# MAGIC
# MAGIC | Scenario | What happens |
# MAGIC |---|---|
# MAGIC | First run | Every chunk is written and recorded as SUCCESS in `ops.chunk_manifest` |
# MAGIC | Re-run, nothing changed | Every chunk is **skipped**; no new files, so COPY INTO / Auto Loader never load rows twice |
# MAGIC | Re-run after a partial failure | Only FAILED or missing chunks are written |
# MAGIC | Cut-off dates changed | Run stops with a clear error unless `force_rechunk = true` |
# MAGIC | `force_rechunk = true` | All chunks are overwritten on purpose (Bronze must then be reset) |
# MAGIC | Output folder deleted by hand | Detected (folder missing) and re-written |

# COMMAND ----------

import json, math, os, sys, traceback
from datetime import datetime, timezone
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.chunking import (CHUNK_COL, CHUNK_TARGETS, REFERENCE_FILES, TIME_SERIES_FILES,
                                       chunk_output_path, invalid_xml_names, parse_cutoffs,
                                       plan_action, reconcile_counts, with_chunk_no)
from vstone_framework.schema_checks import assert_columns_match

# ---- Parameters (job parameters override these defaults) ----
dbutils.widgets.text("catalog", "vstone")
dbutils.widgets.text("landing_schema", "landing")
dbutils.widgets.text("raw_volume", "raw")
dbutils.widgets.text("chunks_volume", "chunks")
dbutils.widgets.text("ops_schema", "ops")
dbutils.widgets.text("chunk1_end_date", "2005-12-31")
dbutils.widgets.text("chunk2_end_date", "2012-12-31")
dbutils.widgets.text("chunk3_end_date", "2015-12-31")
dbutils.widgets.dropdown("force_rechunk", "false", ["false", "true"])
dbutils.widgets.text("rows_per_file", "250000")
dbutils.widgets.text("run_id", "manual")

w = dbutils.widgets.get
catalog, landing = w("catalog"), w("landing_schema")
raw_path = f"/Volumes/{catalog}/{landing}/{w('raw_volume')}"
chunks_root = f"/Volumes/{catalog}/{landing}/{w('chunks_volume')}"
manifest_table = f"{catalog}.{w('ops_schema')}.chunk_manifest"
cutoffs = parse_cutoffs([w("chunk1_end_date"), w("chunk2_end_date"), w("chunk3_end_date")])
cutoff_key = ",".join(c.isoformat() for c in cutoffs)
force = w("force_rechunk") == "true"
rows_per_file = int(w("rows_per_file"))
run_id = w("run_id")
print(f"run_id={run_id} cut-offs={cutoff_key} force={force}")

# COMMAND ----------

# MAGIC %md ## Manifest table: the memory that makes re-runs safe

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {manifest_table} (
  source_file STRING  COMMENT 'Raw file name in the raw Volume',
  level       STRING  COMMENT 'state, metro, county, city, neighborhood, zip or reference name',
  chunk_no    INT     COMMENT '1-4; 1 for reference files',
  file_format STRING  COMMENT 'csv, json or xml',
  output_path STRING  COMMENT 'Folder the chunk was written to',
  row_count   BIGINT  COMMENT 'Rows written, verified by reading the output back',
  min_date    STRING, max_date STRING,
  cutoffs     STRING  COMMENT 'Cut-off dates used, to detect configuration changes',
  status      STRING  COMMENT 'SUCCESS or FAILED',
  error       STRING,
  run_id      STRING,
  updated_at  TIMESTAMP
) COMMENT 'One row per (source file, chunk): status of the Data Chunking job'
""")

def manifest_lookup():
    return {(r.source_file, r.chunk_no): r for r in spark.table(manifest_table).collect()}

def path_exists(path):
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:
        return False

def record(source_file, level, chunk_no, fmt, path, rows, min_d, max_d, status, error=None):
    row = [(source_file, level, chunk_no, fmt, path, rows, min_d, max_d, cutoff_key, status,
            error, run_id, datetime.now(timezone.utc))]
    spark.createDataFrame(row, spark.table(manifest_table).schema).createOrReplaceTempView("m_upd")
    spark.sql(f"""MERGE INTO {manifest_table} t USING m_upd s
                  ON t.source_file = s.source_file AND t.chunk_no = s.chunk_no
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")

# Guard: changing cut-offs after a successful split would silently mix two splits downstream
previous = {r.cutoffs for r in manifest_lookup().values() if r.status == "SUCCESS" and r.chunk_no > 0}
previous.discard(None)
if previous and previous != {cutoff_key} and not force:
    raise ValueError(f"Cut-offs changed from {previous} to {cutoff_key}. "
                     "Set force_rechunk=true to re-split (Bronze must then be reset).")

with open(f"{raw_path}/fields_per_level.json") as fh:
    expected_fields = json.load(fh)

# COMMAND ----------

# MAGIC %md ## Split the six time-series files

# COMMAND ----------

def write_chunk(df, fmt, path, n_rows):
    n_files = max(1, math.ceil(n_rows / rows_per_file))
    writer = df.repartition(n_files).write.mode("overwrite")
    if fmt == "csv":
        writer.option("header", True).csv(path)
    elif fmt == "json":
        writer.json(path)                                 # null fields are omitted
    elif fmt == "xml":
        writer.format("xml").option("rootTag", "rows").option("rowTag", "row").save(path)

def read_back_count(fmt, path):
    if fmt == "csv":
        return spark.read.option("header", True).csv(path).count()
    if fmt == "json":
        return spark.read.json(path).count()
    return spark.read.format("xml").option("rowTag", "row").load(path).count()

failures, summary = [], []
manifest = manifest_lookup()

for file_name, level in TIME_SERIES_FILES.items():
    try:
        df = spark.read.option("header", True).option("inferSchema", False).csv(f"{raw_path}/{file_name}")
        assert_columns_match(level, expected_fields[file_name.split("_")[0]], df.columns)
        bad = invalid_xml_names(df.columns)
        if bad:
            raise ValueError(f"Columns not valid as XML element names: {bad}")

        df = df.transform(with_chunk_no(cutoffs))           # adds chunk_no 1..4
        stats = {r[CHUNK_COL]: r for r in df.groupBy(CHUNK_COL).agg(
                    F.count(F.lit(1)).alias("n"), F.min("Date").alias("lo"), F.max("Date").alias("hi")).collect()}
        if None in stats:
            raise ValueError(f"{stats[None].n} rows have an unparseable Date")
        source_rows = sum(r.n for r in stats.values())

        written = {}
        for chunk_no, target in CHUNK_TARGETS.items():
            path = chunk_output_path(chunks_root, chunk_no, level)
            n = stats[chunk_no].n if chunk_no in stats else 0
            prev = manifest.get((file_name, chunk_no))
            action = plan_action(prev.status if prev else None, path_exists(path), force)
            if action == "skip":
                written[chunk_no] = prev.row_count
                summary.append((file_name, chunk_no, target.file_format, "skipped", prev.row_count))
                continue
            write_chunk(df.filter(F.col(CHUNK_COL) == chunk_no).drop(CHUNK_COL), target.file_format, path, n)
            got = read_back_count(target.file_format, path)
            if got != n:
                raise AssertionError(f"{file_name} chunk {chunk_no}: wrote {n}, read back {got}")
            written[chunk_no] = got
            record(file_name, level, chunk_no, target.file_format, path, got,
                   stats[chunk_no].lo if n else None, stats[chunk_no].hi if n else None, "SUCCESS")
            summary.append((file_name, chunk_no, target.file_format, action, got))

        reconcile_counts(source_rows, written)
        # a successful run clears the file-level FAILED marker left by an earlier attempt
        spark.sql(f"DELETE FROM {manifest_table} WHERE source_file = '{file_name}' AND chunk_no = 0")
        print(f"OK {file_name}: {source_rows:,} rows -> {written}")
    except Exception as e:
        failures.append(file_name)
        record(file_name, level, 0, None, None, None, None, None, "FAILED", str(e)[:2000])
        print(f"FAILED {file_name}: {e}\n{traceback.format_exc()}")

# COMMAND ----------

# MAGIC %md ## Copy the crosswalk (reference) files unchanged

# COMMAND ----------

for file_name, name in REFERENCE_FILES.items():
    path = chunk_output_path(chunks_root, 1, name)
    try:
        prev = manifest.get((file_name, 1))
        action = plan_action(prev.status if prev else None, path_exists(path), force)
        if action == "skip":
            summary.append((file_name, 1, "csv", "skipped", prev.row_count))
            continue
        dbutils.fs.rm(path, True)
        dbutils.fs.cp(f"{raw_path}/{file_name}", f"{path}/{file_name}")
        n = spark.read.option("header", True).csv(path).count()
        record(file_name, name, 1, "csv", path, n, None, None, "SUCCESS")
        summary.append((file_name, 1, "csv", action, n))
    except Exception as e:
        failures.append(file_name)
        record(file_name, name, 1, "csv", path, None, None, None, "FAILED", str(e)[:2000])
        print(f"FAILED {file_name}: {e}")

# COMMAND ----------

display(spark.createDataFrame(summary, "source_file STRING, chunk_no INT, format STRING, action STRING, rows LONG"))
if failures:
    raise RuntimeError(f"Data Chunking failed for {failures}; see {manifest_table}")   # fails the job -> email
print("Data Chunking completed.")
