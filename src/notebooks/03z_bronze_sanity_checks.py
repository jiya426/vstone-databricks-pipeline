# Databricks notebook source
# MAGIC %md
# MAGIC # 03z_bronze_sanity_checks - sanity checks after Bronze ingestion
# MAGIC Runs after every Bronze ingestion task. For each Bronze table it checks:
# MAGIC | Check | Rule |
# MAGIC |---|---|
# MAGIC | row_count_matches_source | rows loaded by this method = rows in the chunk (from `ops.chunk_manifest`) |
# MAGIC | no_null_keys | key columns (Date, RegionName / FIPS / Unique_City_ID) never null |
# MAGIC | no_duplicate_keys | each key appears once - proves a re-run did not load files twice |
# MAGIC | audit_columns_populated | load_dt, source, source_file, batch_id never null |
# MAGIC | columns_match_contract | exactly the contracted columns + audit columns |
# MAGIC
# MAGIC Results go to `ops.sanity_check_results`; any FAIL fails the task (and the job emails you).

# COMMAND ----------

import json, os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from pyspark.sql import functions as F
from vstone_framework.audit import AUDIT_COLUMNS
from vstone_framework.bronze import CROSSWALK_COLUMNS, copy_into_sources
from vstone_framework.ops import ensure_ops_tables, write_check_results
from vstone_framework.quality import failed, run_sanity_checks

dbutils.widgets.text("catalog", "vstone")
dbutils.widgets.text("landing_schema", "landing")
dbutils.widgets.text("raw_volume", "raw")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("ops_schema", "ops")
dbutils.widgets.text("run_id", "manual")
w = dbutils.widgets.get
catalog = w("catalog")
bronze, ops = f"{catalog}.{w('bronze_schema')}", f"{catalog}.{w('ops_schema')}"
raw_path = f"/Volumes/{catalog}/{w('landing_schema')}/{w('raw_volume')}"
run_id = w("run_id")
ensure_ops_tables(spark, ops)

with open(f"{raw_path}/fields_per_level.json") as fh:
    level_fields = {k.lower(): v for k, v in json.load(fh).items()}
manifest = {(r.level, r.chunk_no): r.row_count
            for r in spark.table(f"{ops}.chunk_manifest").filter("status = 'SUCCESS'").collect()}

# COMMAND ----------

all_results = []
for src in copy_into_sources():
    table = f"{bronze}.{src.table}"
    cols = CROSSWALK_COLUMNS.get(src.entity) or level_fields[src.entity]
    df = spark.table(table).filter(F.col("source") == "copy_into_csv")
    all_results += run_sanity_checks(df, table, src.key_columns, manifest.get((src.entity, 1)), cols, AUDIT_COLUMNS)

write_check_results(spark, ops, run_id, "bronze", all_results)
display(spark.createDataFrame([r.__dict__ for r in all_results]))

bad = failed(all_results)
if bad:
    raise AssertionError("Sanity checks failed:\n" + "\n".join(f"{r.table_name} {r.check_name}: expected {r.expected}, got {r.actual}" for r in bad))
print(f"All {len(all_results)} sanity checks passed.")