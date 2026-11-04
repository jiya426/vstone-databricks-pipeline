# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05a_silver_conformed - Day 5: business rules
# MAGIC
# MAGIC | Output | What the business rules do |
# MAGIC |---|---|
# MAGIC | `silver.metric_catalog` | one row per metric: family, segment, **unit**, **currency**, scale, precision, definition |
# MAGIC | `silver.region_reference` | **region standardisation**: names and hierarchy (city -> county -> metro -> state) |
# MAGIC | `silver.region_metric_monthly` | long table: one row per region, month and metric; **currency normalisation** (fx to USD, precision per unit, tier share 0-1 -> 0-100), **date standardisation** (date_key, year, quarter, month) |
# MAGIC
# MAGIC Reconciliation: long rows per level = non-NULL metric cells in the wide Silver table.
# MAGIC Liquid Clustering on (level, metric_name, date_key) for the long table.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from functools import reduce
from pyspark.sql import functions as F
import vstone_framework.conformed_schema as S
from vstone_framework.business_rules import CATALOG_COLUMNS, apply_catalog, metric_catalog, non_null_cells, region_code, unpivot_metrics
from vstone_framework.config import load_config
from vstone_framework.headers import to_snake_case
from vstone_framework.metadata import load_data_dictionary
from vstone_framework.ops import ensure_ops_tables
from vstone_framework.regions import REGION_COLUMNS, build_region_reference
from vstone_framework.runner import RESULTS_SCHEMA, raise_if_failed, results_rows, run_log_logger, run_per_table
from vstone_framework.runtime import get_context
from vstone_framework.uc import comment_table, load_level_fields, set_tags, table_ddl

cfg = load_config()
rules, scfg, ccfg = cfg["business_rules"], cfg["silver"], cfg["silver"]["conformed"]
ctx = get_context(dbutils)
ensure_ops_tables(spark, ctx.ops)
level_fields = load_level_fields(f"{ctx.raw_path}/fields_per_level.json")
dictionary = load_data_dictionary(f"{ctx.raw_path}/DataDictionary.csv")
wide = lambda level: f"{ctx.silver}.{level}_{scfg['time_series_suffix']}"
catalog_table = f"{ctx.silver}.{ccfg['metric_catalog_table']}"
long_table = f"{ctx.silver}.{ccfg['long_table']}"
region_table = f"{ctx.silver}.{ccfg['region_table']}"
recon_table = f"{ctx.ops}.{ccfg['reconciliation_table']}"

def create(table, columns, comment, cluster_by=None, entity=None):
    spark.sql(table_ddl(table, columns, comment, cluster_by))
    comment_table(spark, table, comment)
    set_tags(spark, table, {**cfg["tags"], "layer": "silver", "entity": entity or table.split(".")[-1]})

# COMMAND ----------

# MAGIC %md ## 1. Metric catalogue (units, currency, precision)

# COMMAND ----------

codes = {c for fields in level_fields.values() for c in fields if c not in ("Date", "RegionName")}
catalog_rows = metric_catalog(codes, dictionary, rules)
create(catalog_table, S.METRIC_CATALOG, "Silver: catalogue of the 96 Zillow metrics with unit, currency, "
       "normalisation scale, precision and business definition (drives value normalisation)")
catalog_df = spark.createDataFrame([tuple(r[c] for c in CATALOG_COLUMNS) for r in catalog_rows],
                                   ", ".join(f"{n} {t}" for n, t, _ in S.METRIC_CATALOG))
catalog_df.write.insertInto(catalog_table, overwrite=True)
display(spark.table(catalog_table).groupBy("unit", "currency_code", "decimal_places").count().orderBy("unit"))

# COMMAND ----------

# MAGIC %md ## 2. Region reference (region standardisation)

# COMMAND ----------

regions = reduce(lambda a, b: a.unionByName(b), [
    spark.table(wide(lvl)).select(F.lit(lvl).alias("level"),
                                  region_code(lvl, F.col("region_name"), rules).alias("region_code")).distinct()
    for lvl in cfg["levels"]])
reference = (build_region_reference(regions, spark.table(f"{ctx.silver}.county_crosswalk"),
                                    spark.table(f"{ctx.silver}.city_crosswalk"), rules)
             .withColumn("load_dt", F.current_timestamp())
             .withColumn("source", F.lit("business_rules.region_standardisation"))
             .withColumn("source_file", F.lit(f"{ctx.silver}.county_crosswalk, {ctx.silver}.city_crosswalk"))
             .withColumn("batch_id", F.lit(ctx.run_id)))
create(region_table, S.REGION, "Silver: one row per region and level with standardised code, display name and "
       "geography hierarchy (source for the Gold region dimension)")
reference.select(*[c for c, _, _ in S.REGION]).write.insertInto(region_table, overwrite=True)
display(spark.table(region_table).groupBy("level").agg(F.count("*").alias("regions"),
        F.sum(F.col("is_resolved").cast("int")).alias("resolved")).orderBy("level"))

# COMMAND ----------

# MAGIC %md ## 3. Long table with normalised values and standard dates

# COMMAND ----------

create(long_table, S.LONG, "Silver: one row per region, month and metric with normalised value, unit, currency "
       "and standard date attributes (NULL metric cells removed)", cluster_by=ccfg["long_cluster_by"])
spark.sql(f"""CREATE TABLE IF NOT EXISTS {recon_table} (
  run_id STRING, level STRING, expected_rows BIGINT, actual_rows BIGINT, balanced BOOLEAN, checked_at TIMESTAMP)
  COMMENT 'Wide Silver -> long Silver reconciliation: non-NULL metric cells = long rows'""")
catalog = spark.table(catalog_table)

def build_long(level):
    src = spark.table(wide(level))
    metrics = [to_snake_case(c) for c in level_fields[level] if c not in ("Date", "RegionName")]
    expected = non_null_cells(src, metrics)
    (src.transform(unpivot_metrics(level, metrics, rules)).transform(apply_catalog(catalog, rules, ctx.run_id))
        .write.mode("overwrite").option("replaceWhere", f"level = '{level}'").saveAsTable(long_table))
    actual = spark.table(long_table).filter(F.col("level") == level).count()
    spark.createDataFrame([(ctx.run_id, level, expected, actual, expected == actual)],
                          "run_id STRING, level STRING, expected_rows BIGINT, actual_rows BIGINT, balanced BOOLEAN")          .withColumn("checked_at", F.current_timestamp()).write.mode("append").saveAsTable(recon_table)
    print(f"{level}: {actual:,} long rows (expected {expected:,} non-NULL cells)")
    if expected != actual:
        raise AssertionError(f"{level}: expected {expected:,} rows, wrote {actual:,}")
    return actual, None

results = run_per_table(cfg["levels"], lambda l: f"{long_table} [{l}]", build_long,
                        run_log_logger(spark, ctx.ops, ctx.run_id, "silver", "silver_conformed"))
display(spark.createDataFrame(results_rows(results), RESULTS_SCHEMA))
raise_if_failed(results, "Silver conformed", f"{ctx.ops}.pipeline_run_log")
