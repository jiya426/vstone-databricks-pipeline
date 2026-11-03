# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03z_bronze_sanity_checks - sanity checks after Bronze ingestion (all 4 methods)
# MAGIC Runs after every Bronze ingestion task. For each of the 26 Bronze tables it checks:
# MAGIC | Check | Rule |
# MAGIC |---|---|
# MAGIC | row_count_matches_source | rows loaded by this method = rows in that chunk (`ops.chunk_manifest`) - reconciliation |
# MAGIC | no_null_keys | key columns (Date, RegionName / FIPS / Unique_City_ID) never null |
# MAGIC | no_duplicate_keys | each key appears once - proves a re-run did not load files twice |
# MAGIC | audit_columns_populated | load_dt, source, source_file, batch_id never null |
# MAGIC | no_rescued_data | JSON / XML / DLT tables: nothing fell outside the expected schema |
# MAGIC | columns_match_contract | exactly the contracted columns (+ _rescued_data) + audit columns |
# MAGIC
# MAGIC Results go to `ops.sanity_check_results`; any FAIL fails the task (and the job emails you).

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from pyspark.sql import functions as F
from vstone_framework.audit import AUDIT_COLUMNS
from vstone_framework.bronze import RESCUED_COL, bronze_sources, contract_columns
from vstone_framework.ops import ensure_ops_tables, write_check_results
from vstone_framework.quality import failed, run_sanity_checks
from vstone_framework.uc import load_level_fields

for name, default in [("catalog", "vstone"), ("landing_schema", "landing"), ("raw_volume", "raw"),
                      ("bronze_schema", "bronze"), ("ops_schema", "ops"), ("run_id", "manual"),
                      ("methods", "copy_into,dlt,autoloader,pyspark_xml")]:
    dbutils.widgets.text(name, default)
w = dbutils.widgets.get
catalog = w("catalog")
bronze, ops = f"{catalog}.{w('bronze_schema')}", f"{catalog}.{w('ops_schema')}"
raw_path = f"/Volumes/{catalog}/{w('landing_schema')}/{w('raw_volume')}"
run_id = w("run_id")
methods = [m.strip() for m in w("methods").split(",") if m.strip()]
ensure_ops_tables(spark, ops)

level_fields = load_level_fields(f"{raw_path}/fields_per_level.json")
manifest = {(r.level, r.chunk_no): r.row_count
            for r in spark.table(f"{ops}.chunk_manifest").filter("status = 'SUCCESS'").collect()}

# COMMAND ----------

all_results = []
for src in bronze_sources(methods):
    table = f"{bronze}.{src.table}"
    df = spark.table(table).filter(F.col("source") == src.source)
    all_results += run_sanity_checks(df, table, src.key_columns, manifest.get((src.entity, src.chunk_no)),
                                     contract_columns(src, level_fields), AUDIT_COLUMNS,
                                     rescued_column=RESCUED_COL if src.has_rescued else None)

write_check_results(spark, ops, run_id, "bronze", all_results)
display(spark.createDataFrame([r.__dict__ for r in all_results]))

bad = failed(all_results)
if bad:
    raise AssertionError("Sanity checks failed:\n" + "\n".join(
        f"{r.table_name} {r.check_name}: expected {r.expected}, got {r.actual}" for r in bad))
print(f"All {len(all_results)} sanity checks passed across {len(bronze_sources(methods))} Bronze tables.")
