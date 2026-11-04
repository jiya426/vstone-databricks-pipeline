# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04z_silver_sanity_checks - Silver data quality and reconciliation
# MAGIC | Check | Rule |
# MAGIC |---|---|
# MAGIC | row_count_matches_source | table rows = silver_rows recorded by this run's reconciliation |
# MAGIC | no_null_keys / no_duplicate_keys | (date, region_name) - or fips / unique_city_id - unique and not NULL |
# MAGIC | audit_columns_populated | load_dt, source, source_file, batch_id never NULL |
# MAGIC | columns_match_contract | exactly the snake_case columns + audit columns |
# MAGIC | reconciliation_balanced | bronze = silver + quarantined + empty + duplicates |
# MAGIC | quarantine_matches_expected | quarantined rows per level = profiled defects (302 neighborhood, 0 elsewhere) |

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from pyspark.sql import Window, functions as F
from vstone_framework.audit import AUDIT_COLUMNS
from vstone_framework.bronze import CROSSWALK_COLUMNS
from vstone_framework.config import load_config
from vstone_framework.headers import to_snake_case
from vstone_framework.ops import ensure_ops_tables, write_check_results
from vstone_framework.quality import CheckResult, failed, run_sanity_checks
from vstone_framework.runtime import get_context
from vstone_framework.silver import REFERENCE_RULES
from vstone_framework.uc import load_level_fields

cfg, scfg = load_config(), load_config()["silver"]
ctx = get_context(dbutils)
ensure_ops_tables(spark, ctx.ops)
level_fields = load_level_fields(f"{ctx.raw_path}/fields_per_level.json")
latest = Window.partitionBy("level").orderBy(F.col("checked_at").desc())
recon = {r.level: r for r in spark.table(f"{ctx.ops}.{scfg['reconciliation_table']}")
         .withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").collect()}
quarantine = {r.level: r["count"] for r in
              spark.table(f"{ctx.silver}.{scfg['quarantine_table']}").groupBy("level").count().collect()}

# COMMAND ----------

results = []
for level in cfg["levels"]:
    table = f"{ctx.silver}.{level}_{scfg['time_series_suffix']}"
    cols = [to_snake_case(c) for c in level_fields[level]]
    r = recon[level]
    results += run_sanity_checks(spark.table(table), table, scfg["key_columns"], r.silver_rows, cols, AUDIT_COLUMNS)
    results.append(CheckResult(table, "reconciliation_balanced", "True", str(r.balanced), "PASS" if r.balanced else "FAIL"))
    exp_q, got_q = scfg["expected_quarantine_rows"][level], quarantine.get(level, 0)
    results.append(CheckResult(table, "quarantine_matches_expected", str(exp_q), str(got_q),
                               "PASS" if exp_q == got_q else "FAIL"))

for name, rules in REFERENCE_RULES.items():
    table = f"{ctx.silver}.{name}"
    bronze_rows = spark.table(f"{ctx.bronze}.{name}").count()
    results += run_sanity_checks(spark.table(table), table, rules["key"], bronze_rows,
                                 [to_snake_case(c) for c in CROSSWALK_COLUMNS[name]], AUDIT_COLUMNS)

write_check_results(spark, ctx.ops, ctx.run_id, "silver", results)
display(spark.createDataFrame([x.__dict__ for x in results]))
bad = failed(results)
if bad:
    raise AssertionError("Silver checks failed:\n" + "\n".join(
        f"{x.table_name} {x.check_name}: expected {x.expected}, got {x.actual}" for x in bad))
print(f"All {len(results)} Silver checks passed across {len(cfg['levels']) + len(REFERENCE_RULES)} tables.")
