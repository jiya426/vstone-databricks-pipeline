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

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Job parameters arrive as widgets (values from `databricks.yml` variables); the defaults come from
# MAGIC `conf/vstone_config.json` and are used only when the notebook is run by hand.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("landing_schema", d["landing_schema"], "Landing schema")
dbutils.widgets.text("bronze_schema", d["bronze_schema"], "Bronze schema")
dbutils.widgets.text("silver_schema", d["silver_schema"], "Silver schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("raw_volume", d["raw_volume"], "Raw files Volume")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")

ctx = get_context(dbutils)

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
ensure_ops_tables(spark, ctx.ops)
dictionary = load_data_dictionary(ctx.metadata_path("data_dictionary"))

# COMMAND ----------

def build_reference(name):
    table = f"{ctx.silver}.{name}"
    bronze_df = spark.table(f"{ctx.bronze}.{name}").filter(F.col("source") == cfg["reference_load"]["source"])
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
raise_if_failed(results, "Silver reference", ctx.ops_table("run_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Cleaned the 2 crosswalks: snake_case headers, trimmed text, zero-padded codes, upper-case state codes.
# MAGIC - Dropped null keys, kept one row per key, and wrote documented Silver reference tables.
