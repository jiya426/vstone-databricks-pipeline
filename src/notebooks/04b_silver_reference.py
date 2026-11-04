# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04b_silver_reference - Day 4: crosswalks Bronze -> Silver
# MAGIC Cleans `county_crosswalk` and `city_crosswalk` with `silver.clean_reference`: snake_case headers
# MAGIC (pandas UDF), trimmed text, zero-padded codes (FIPS 5, state FIPS 2, county FIPS 3), upper-case state codes,
# MAGIC NULL keys removed, one row per key. Reconciliation: bronze = silver + removed.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from pyspark.sql import functions as F
from vstone_framework.bronze import CROSSWALK_COLUMNS, create_table_ddl
from vstone_framework.config import load_config
from vstone_framework.headers import header_mapping
from vstone_framework.metadata import column_comment, load_data_dictionary
from vstone_framework.ops import ensure_ops_tables
from vstone_framework.runner import RESULTS_SCHEMA, raise_if_failed, results_rows, run_log_logger, run_per_table
from vstone_framework.runtime import get_context
from vstone_framework.silver import REFERENCE_RULES, clean_reference
from vstone_framework.uc import comment_table, set_tags

cfg, scfg = load_config(), load_config()["silver"]
ctx = get_context(dbutils)
ensure_ops_tables(spark, ctx.ops)
dictionary = load_data_dictionary(f"{ctx.raw_path}/DataDictionary.csv")

# COMMAND ----------

def build_reference(name):
    table = f"{ctx.silver}.{name}"
    bronze_df = spark.table(f"{ctx.bronze}.{name}").filter(F.col("source") == "copy_into_csv")
    bronze_rows = bronze_df.count()
    mapping = header_mapping(spark, CROSSWALK_COLUMNS[name], method=scfg["header_udf"])
    comments = {mapping[c]: column_comment(c, dictionary) for c in CROSSWALK_COLUMNS[name]}
    description = f"Silver: cleaned Zillow {name.replace('_', ' ')} (standardised headers, padded codes, unique keys)."
    spark.sql(create_table_ddl(table, list(mapping.values()), comments, description))
    comment_table(spark, table, description)
    set_tags(spark, table, {**cfg["tags"], "layer": "silver", "entity": name})
    bronze_df.transform(clean_reference(mapping, REFERENCE_RULES[name], ctx.run_id)).write.insertInto(table, overwrite=True)
    silver_rows = spark.table(table).count()
    print(f"{name}: bronze {bronze_rows:,} -> silver {silver_rows:,} (removed {bronze_rows - silver_rows:,})")
    return silver_rows, None

results = run_per_table(list(REFERENCE_RULES), lambda n: f"{ctx.silver}.{n}", build_reference,
                        run_log_logger(spark, ctx.ops, ctx.run_id, "silver", "silver_reference"))
display(spark.createDataFrame(results_rows(results), RESULTS_SCHEMA))
raise_if_failed(results, "Silver reference", f"{ctx.ops}.pipeline_run_log")