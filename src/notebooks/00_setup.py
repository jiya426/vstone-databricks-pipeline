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
dbutils.widgets.text("gold_schema", d["gold_schema"], "Gold schema")
dbutils.widgets.text("governance_schema", d["governance_schema"], "Governance schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("raw_volume", d["raw_volume"], "Raw files Volume")
dbutils.widgets.text("chunks_volume", d["chunks_volume"], "Chunks Volume")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")

ctx = get_context(dbutils)

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.config import load_config
from vstone_framework.metadata import sql_string
from vstone_framework.runtime import get_context

cfg = load_config()
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
           ctx.chunks_volume: "The 4 landed chunk files: chunk1_csv, chunk2_csv, chunk3_json, chunk4_xml (one file each)",
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Created the Unity Catalog objects the project needs: catalog, the 6 schemas and the landing Volumes, all with comments.
# MAGIC - Every name came from job parameters (bundle variables), so the same notebook builds dev or prod.
