# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06b_business_glossary - business glossary (PDF Day 6)
# MAGIC **Goal:** one governed table that explains every business term a Gold user meets:
# MAGIC
# MAGIC | Category | Terms | Definition from |
# MAGIC |---|---|---|
# MAGIC | Metric | the 96 Zillow metrics (ZHVI, ZRI, listing prices, inventory, sales ...) | Zillow data dictionary via `silver.metric_catalog` |
# MAGIC | Feature | MoM / YoY growth, rent yield, tier spread, listing premium | Silver feature rules |
# MAGIC | Geography level | state, metro, county, city, neighborhood, zip | config `gold.level_definitions` |
# MAGIC | Key | region_key, date_key | Gold data model |
# MAGIC
# MAGIC Written to `governance.business_glossary`, with table/column comments and tags. Nothing is typed by hand here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("silver_schema", d["silver_schema"], "Silver schema")
dbutils.widgets.text("gold_schema", d["gold_schema"], "Gold schema")
dbutils.widgets.text("governance_schema", d["governance_schema"], "Governance schema")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")

ctx = get_context(dbutils)

# COMMAND ----------

from vstone_framework.config import load_config
from vstone_framework.features import FEATURES
from vstone_framework.gold import glossary_rows
from vstone_framework.gold_schema import GLOSSARY, TABLE_COMMENTS
from vstone_framework.uc import comment_table, set_tags, table_ddl

cfg = load_config()
g, T = cfg["gold"], cfg["gold"]["tables"]
catalog_rows = [r.asDict() for r in spark.table(f"{ctx.silver}.{g['sources']['metric']}").collect()]
rows = glossary_rows(catalog_rows, FEATURES, g["level_definitions"], T["fact"], T["agg_market_trend"],
                     T["dim_region_current"])

table = f"{ctx.governance}.{g['glossary_table']}"
spark.sql(table_ddl(table, GLOSSARY, TABLE_COMMENTS["glossary"]))
(spark.createDataFrame(rows, ", ".join(f"{n} {t}" for n, t, _ in GLOSSARY))
      .write.mode("overwrite").saveAsTable(table))            # same schema: column comments are kept
comment_table(spark, table, TABLE_COMMENTS["glossary"])
set_tags(spark, table, {**cfg["tags"], "layer": "governance", "entity": "business_glossary"})
display(spark.table(table).groupBy("category").count())
print(f"{len(rows)} glossary terms written to {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Built the business glossary from existing metadata: metric definitions from the Zillow data dictionary,
# MAGIC   feature and level definitions from the project rules, keys from the Gold data model.
# MAGIC - Stored it as the governed table `governance.business_glossary` with comments and tags, ready for Genie and analysts.
