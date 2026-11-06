# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05b_silver_features - Day 5: meaningful features per region and month
# MAGIC | Feature | Definition |
# MAGIC |---|---|
# MAGIC | zhvi_mom_pct | month-over-month change in typical home value, % |
# MAGIC | zhvi_yoy_pct | year-over-year change in typical home value, % |
# MAGIC | zri_yoy_pct | year-over-year change in typical rent, % |
# MAGIC | gross_rent_yield_pct | rent x 12 / home value x 100 - affordability / investor yield |
# MAGIC | tier_spread_ratio | top-tier / bottom-tier value - price inequality |
# MAGIC | listing_premium_pct | asking price vs typical value, % |
# MAGIC Lags count only when the earlier month is exactly 1 or 12 months back (gaps never give a wrong change).

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
dbutils.widgets.text("silver_schema", d["silver_schema"], "Silver schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")
# Task parameter: region shown in the preview at the end
dbutils.widgets.text("demo_region", d["demo_region"], "Preview region (state)")

ctx = get_context(dbutils, ["demo_region"])

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from pyspark.sql import functions as F
import vstone_framework.conformed_schema as S
from vstone_framework.config import load_config
from vstone_framework.features import compute_features
from vstone_framework.ops import ensure_ops_tables
from vstone_framework.runner import RESULTS_SCHEMA, raise_if_failed, results_rows, run_log_logger, run_per_table
from vstone_framework.runtime import get_context
from vstone_framework.uc import comment_table, set_tags, table_ddl

cfg = load_config()
rules, scfg, ccfg = cfg["business_rules"], cfg["silver"], cfg["silver"]["conformed"]
ensure_ops_tables(spark, ctx.ops)
table = f"{ctx.silver}.{ccfg['features_table']}"
comment = "Silver: engineered features per region and month (value and rent growth, rent yield, tier spread, listing premium)"
spark.sql(table_ddl(table, S.FEATURE_TABLE, comment, cluster_by=["level", "region_key"]))
comment_table(spark, table, comment)
set_tags(spark, table, {**cfg["tags"], "layer": "silver", "entity": "features"})

# COMMAND ----------

def build_features(level):
    src = spark.table(f"{ctx.silver}.{level}_{scfg['time_series_suffix']}")
    (src.transform(compute_features(level, rules, ctx.run_id))
        .write.mode("overwrite").option("replaceWhere", f"level = '{level}'").saveAsTable(table))
    rows = spark.table(table).filter(F.col("level") == level).count()
    return rows, None

results = run_per_table(cfg["levels"], lambda l: f"{table} [{l}]", build_features,
                        run_log_logger(spark, ctx.ops, ctx.run_id, "silver", "silver_features"))
display(spark.createDataFrame(results_rows(results), RESULTS_SCHEMA))
display(spark.table(table).filter(f"region_key = 'state:{ctx.param('demo_region')}' AND month = 1").orderBy("date")
        .select("date", "zhvi_all_homes", "zhvi_yoy_pct", "gross_rent_yield_pct", "tier_spread_ratio"))
raise_if_failed(results, "Silver features", ctx.ops_table("run_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Computed meaningful features per region and month: MoM and YoY growth, price-to-rent, rent yield, tier spread, listing discount.
