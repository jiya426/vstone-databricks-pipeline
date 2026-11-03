# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00_setup - Vstone Unity Catalog objects
# MAGIC Creates the catalog, one schema per layer (names and descriptions from `conf/vstone_config.json`)
# MAGIC and the landing Volumes. Safe to re-run: every statement uses IF NOT EXISTS.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.config import load_config
from vstone_framework.metadata import sql_string
from vstone_framework.runtime import get_context

cfg = load_config()
ctx = get_context(dbutils)
schema_names = {"landing": ctx.landing_schema, "bronze": ctx.bronze_schema, "silver": ctx.silver_schema,
                "gold": ctx.gold_schema, "governance": ctx.governance_schema, "ops": ctx.ops_schema}

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {ctx.catalog} COMMENT 'Vstone Zillow housing analytics project'")
except Exception as e:
    raise RuntimeError(f"Could not create catalog '{ctx.catalog}'. If your workspace does not allow it, "
                       f"set the 'catalog' widget to your existing workspace catalog. Original error: {e}")

for key, schema in schema_names.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.fq(schema)} COMMENT {sql_string(cfg['schemas'][key])}")

volumes = {ctx.raw_volume: "Original archive (14) files exactly as downloaded",
           ctx.chunks_volume: "Generated chunks: csv_initial, csv_incremental, json, xml",
           cfg["volumes"]["checkpoints"]: "Auto Loader / streaming checkpoints and schema locations"}
for vol, comment in volumes.items():
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {ctx.landing}.{vol} COMMENT {sql_string(comment)}")

# COMMAND ----------

# MAGIC %md ## Validation

# COMMAND ----------

found = {r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {ctx.catalog}").collect()}
missing = set(schema_names.values()) - found
assert not missing, f"Missing schemas: {missing}"
vols = {r.volume_name for r in spark.sql(f"SHOW VOLUMES IN {ctx.landing}").collect()}
assert set(volumes) <= vols, f"Missing volumes, found {vols}"
print(f"OK: catalog {ctx.catalog}, schemas {sorted(found & set(schema_names.values()))}, volumes {sorted(vols)}")
