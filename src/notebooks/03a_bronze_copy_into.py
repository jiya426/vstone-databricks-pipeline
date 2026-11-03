# Databricks notebook source
# MAGIC %md
# MAGIC # 03a_bronze_copy_into - Day 2: CSV chunk 1 + crosswalks into Bronze with COPY INTO
# MAGIC
# MAGIC **What:** loads `chunks/csv_initial/<level>/` (6 levels) and the 2 crosswalk files into Delta tables in
# MAGIC `vstone.bronze` with the SQL `COPY INTO` command.
# MAGIC
# MAGIC **Why COPY INTO:** it is idempotent - Delta records which files were already loaded, so re-running
# MAGIC the job loads 0 new rows instead of duplicating data.
# MAGIC
# MAGIC **Steps per table**
# MAGIC 1. Schema validation: the CSV header must equal the contract (`fields_per_level.json` / crosswalk list).
# MAGIC 2. `CREATE TABLE IF NOT EXISTS` - all source columns STRING (raw), plus audit columns, every column commented.
# MAGIC 3. Table comment + tags (discoverability).
# MAGIC 4. `COPY INTO` selecting only contracted columns and adding `load_dt`, `source`, `source_file`, `batch_id`.
# MAGIC 5. Result logged to `ops.pipeline_run_log`. Any failure is logged, other tables continue, and the task fails at the end.

# COMMAND ----------

import csv, json, os, sys, traceback
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.bronze import CROSSWALK_COLUMNS, copy_into_sources, copy_into_sql, create_table_ddl
from vstone_framework.metadata import column_comments, load_data_dictionary, sql_string
from vstone_framework.ops import ensure_ops_tables, log_run
from vstone_framework.schema_checks import assert_columns_match

dbutils.widgets.text("catalog", "vstone")
dbutils.widgets.text("landing_schema", "landing")
dbutils.widgets.text("raw_volume", "raw")
dbutils.widgets.text("chunks_volume", "chunks")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("ops_schema", "ops")
dbutils.widgets.text("run_id", "manual")

w = dbutils.widgets.get
catalog = w("catalog")
raw_path = f"/Volumes/{catalog}/{w('landing_schema')}/{w('raw_volume')}"
chunks_root = f"/Volumes/{catalog}/{w('landing_schema')}/{w('chunks_volume')}"
bronze = f"{catalog}.{w('bronze_schema')}"
ops = f"{catalog}.{w('ops_schema')}"
run_id = w("run_id")

ensure_ops_tables(spark, ops)
with open(f"{raw_path}/fields_per_level.json") as fh:
    level_fields = {k.lower(): v for k, v in json.load(fh).items()}       # {"zip": [...], ...}
dictionary = load_data_dictionary(f"{raw_path}/DataDictionary.csv")
print(f"run_id={run_id}, target schema={bronze}")

# COMMAND ----------

def expected_columns(entity):
    return CROSSWALK_COLUMNS[entity] if entity in CROSSWALK_COLUMNS else level_fields[entity]

def header_of_first_csv(folder):
    files = sorted(f.path for f in dbutils.fs.ls(folder) if f.name.endswith(".csv"))
    if not files:
        raise FileNotFoundError(f"No .csv files in {folder}")
    with open(files[0].replace("dbfs:", ""), newline="", encoding="utf-8-sig") as fh:
        return next(csv.reader(fh)), len(files)

def set_tags(table, tags):
    pairs = ", ".join(f"{sql_string(k)} = {sql_string(v)}" for k, v in tags.items())
    try:
        spark.sql(f"ALTER TABLE {table} SET TAGS ({pairs})")
    except Exception as e:                       # tags are optional metadata; never fail the load for them
        print(f"WARN: could not set tags on {table}: {e}")

# COMMAND ----------

results, failures = [], []
for src in copy_into_sources():
    table = f"{bronze}.{src.table}"
    folder = f"{chunks_root}/{src.folder}"
    started = datetime.now(timezone.utc)
    try:
        cols = expected_columns(src.entity)
        header, n_files = header_of_first_csv(folder)
        assert_columns_match(src.entity, cols, header)                     # 1. schema validation

        spark.sql(create_table_ddl(table, cols, column_comments(cols, dictionary), src.description))   # 2
        spark.sql(f"COMMENT ON TABLE {table} IS {sql_string(src.description)}")                        # 3
        set_tags(table, {"layer": "bronze", "source_system": "zillow", "ingestion": "copy_into",
                         "entity": src.entity, "project": "vstone"})

        metrics = spark.sql(copy_into_sql(table, folder, cols, "copy_into_csv", run_id)).first().asDict()  # 4
        inserted = int(metrics.get("num_inserted_rows") or metrics.get("num_affected_rows") or 0)
        log_run(spark, ops, run_id, "bronze", "copy_into_csv", table, inserted, n_files, "SUCCESS", started)  # 5
        results.append((table, n_files, inserted, "SUCCESS", None))
        print(f"OK   {table}: {inserted:,} new rows from {n_files} file(s)")
    except Exception as e:
        failures.append(table)
        log_run(spark, ops, run_id, "bronze", "copy_into_csv", table, None, None, "FAILED", started, str(e))
        results.append((table, None, None, "FAILED", str(e)[:300]))
        print(f"FAIL {table}: {e}\n{traceback.format_exc()}")

display(spark.createDataFrame(results, "table STRING, files INT, new_rows LONG, status STRING, error STRING"))
if failures:
    raise RuntimeError(f"COPY INTO failed for {failures}; details in {ops}.pipeline_run_log")