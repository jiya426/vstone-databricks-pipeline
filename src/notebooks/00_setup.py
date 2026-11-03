# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup - Vstone Unity Catalog objects
# MAGIC Creates the catalog, one schema per layer, and the landing Volumes.
# MAGIC Safe to re-run: every statement uses IF NOT EXISTS.
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `landing` schema + `raw`, `chunks` volumes | Raw files and the 4 chunks |
# MAGIC | `bronze` / `silver` / `gold` | Medallion layers |
# MAGIC | `governance` | Row filters, column masks, access map |
# MAGIC | `ops` | Run log, sanity checks, chunk manifest |

# COMMAND ----------

# Parameters (no hard-coding: change them here or pass them from a job)
dbutils.widgets.text("catalog", "vstone", "Catalog")
dbutils.widgets.text("landing_schema", "landing", "Landing schema")
dbutils.widgets.text("raw_volume", "raw", "Raw volume")
dbutils.widgets.text("chunks_volume", "chunks", "Chunks volume")

catalog = dbutils.widgets.get("catalog")
landing = dbutils.widgets.get("landing_schema")
raw_volume = dbutils.widgets.get("raw_volume")
chunks_volume = dbutils.widgets.get("chunks_volume")

schemas = {
    landing: "Landing zone: Volumes holding raw Zillow files and generated chunks",
    "bronze": "Bronze layer: raw data as Delta, all columns STRING plus audit columns",
    "silver": "Silver layer: typed, cleaned, deduplicated and standardised data",
    "gold": "Gold layer: star schema (facts, SCD2 dimensions) and aggregates",
    "governance": "Security objects: row filters, column masks, access mapping",
    "ops": "Operational tables: run log, sanity checks, chunk manifest",
}

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT 'Vstone Zillow housing analytics project'")
except Exception as e:
    raise RuntimeError(
        f"Could not create catalog '{catalog}'. If your workspace does not allow it, "
        f"set the 'catalog' widget to your existing workspace catalog. Original error: {e}"
    )
spark.sql(f"USE CATALOG {catalog}")

for schema, comment in schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT '{comment}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{landing}.{raw_volume} "
          "COMMENT 'Original archive (14) files exactly as downloaded'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{landing}.{chunks_volume} "
          "COMMENT 'Generated chunks: csv_initial, csv_incremental, json, xml'")

# COMMAND ----------

# MAGIC %md ## Validation

# COMMAND ----------

found = {r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()}
missing = set(schemas) - found
assert not missing, f"Missing schemas: {missing}"

vols = {r.volume_name for r in spark.sql(f"SHOW VOLUMES IN {catalog}.{landing}").collect()}
assert {raw_volume, chunks_volume} <= vols, f"Missing volumes, found {vols}"

print(f"OK: catalog {catalog}, schemas {sorted(found & set(schemas))}, volumes {sorted(vols)}")
