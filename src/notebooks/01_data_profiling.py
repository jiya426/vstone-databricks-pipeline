# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01_data_profiling - Zillow Economics dataset profile
# MAGIC **Purpose:** understand every file before designing the pipeline (PDF Day 1: "Dataset Profiling").
# MAGIC
# MAGIC For each of the 6 time-series files it measures: rows, regions, date range, duplicate keys,
# MAGIC null keys, unparseable dates, header vs `fields_per_level.json`, fill rate and numeric validity
# MAGIC per column, negative values. For the 2 crosswalks: key uniqueness and nulls. Then it checks how
# MAGIC each level's `RegionName` joins to the crosswalks.
# MAGIC
# MAGIC Results are saved to `ops.profile_file_summary`, `ops.profile_column_stats` and
# MAGIC `ops.profile_dq_findings` so they can be queried and shown as evidence.

# COMMAND ----------

import json, os, sys
from datetime import datetime, timezone
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))   # makes src/ importable
from vstone_framework.chunking import TIME_SERIES_FILES, REFERENCE_FILES
from vstone_framework.schema_checks import compare_columns

dbutils.widgets.text("catalog", "vstone")
dbutils.widgets.text("landing_schema", "landing")
dbutils.widgets.text("raw_volume", "raw")
dbutils.widgets.text("ops_schema", "ops")

catalog = dbutils.widgets.get("catalog")
ops = f"{catalog}.{dbutils.widgets.get('ops_schema')}"
raw_path = f"/Volumes/{catalog}/{dbutils.widgets.get('landing_schema')}/{dbutils.widgets.get('raw_volume')}"
profiled_at = datetime.now(timezone.utc)

def read_raw_csv(file_name):
    # All columns as STRING: profiling must see the data exactly as delivered
    return (spark.read.option("header", True).option("inferSchema", False)
            .csv(f"{raw_path}/{file_name}"))

with open(f"{raw_path}/fields_per_level.json") as fh:
    expected_fields = json.load(fh)          # {"City": [...], "Zip": [...], ...}

# COMMAND ----------

# MAGIC %md ## 1. Time-series files: file-level profile

# COMMAND ----------

file_rows, column_rows = [], []

for file_name, level in TIME_SERIES_FILES.items():
    df = read_raw_csv(file_name)
    metric_cols = [c for c in df.columns if c not in ("Date", "RegionName")]

    s = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("RegionName").alias("regions"),
        F.min("Date").alias("min_date"),
        F.max("Date").alias("max_date"),
        F.countDistinct("Date").alias("months"),
        F.sum(F.col("RegionName").isNull().cast("int")).alias("null_region"),
        F.sum(F.col("Date").isNull().cast("int")).alias("null_date"),
        F.sum(F.try_to_timestamp(F.col("Date"), F.lit("yyyy-MM-dd")).isNull().cast("int")).alias("bad_dates"),
    ).first()
    dup_keys = df.groupBy("Date", "RegionName").count().filter("count > 1").count()

    # One pass: filled cells, non-numeric cells and negative cells for every metric column
    exprs = []
    for c in metric_cols:
        v = F.expr(f"try_cast(`{c}` AS DOUBLE)")
        exprs += [F.count(F.col(f"`{c}`")).alias(f"{c}__filled"),
                  F.sum((F.col(f"`{c}`").isNotNull() & v.isNull()).cast("int")).alias(f"{c}__nonnum"),
                  F.sum((v < 0).cast("int")).alias(f"{c}__neg")]
    stats = df.agg(*exprs).first().asDict()

    for c in metric_cols:
        column_rows.append((level, c, s.rows, stats[f"{c}__filled"],
                            round(stats[f"{c}__filled"] / s.rows * 100, 2),
                            stats[f"{c}__nonnum"] or 0, stats[f"{c}__neg"] or 0))

    level_key = file_name.split("_")[0]                     # "Zip", "City", ...
    diff = compare_columns(expected_fields[level_key], df.columns)
    filled_total = sum(stats[f"{c}__filled"] for c in metric_cols)
    file_rows.append((file_name, level, s.rows, s.regions, s.min_date, s.max_date, s.months,
                      dup_keys, s.null_region, s.null_date, s.bad_dates, len(metric_cols),
                      round(filled_total / (s.rows * len(metric_cols)) * 100, 2),
                      ",".join(diff["missing"]) or None, ",".join(diff["unexpected"]) or None))
    print(f"profiled {file_name}: {s.rows:,} rows")

file_schema = ("file_name STRING, level STRING, row_count LONG, distinct_regions LONG, min_date STRING, "
               "max_date STRING, distinct_months LONG, duplicate_keys LONG, null_region LONG, null_date LONG, "
               "bad_dates LONG, metric_columns INT, fill_rate_pct DOUBLE, missing_columns STRING, "
               "unexpected_columns STRING")
file_df = (spark.createDataFrame(file_rows, file_schema)
           .withColumn("profiled_at", F.lit(profiled_at)))
file_df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{ops}.profile_file_summary")
display(file_df.orderBy(F.desc("row_count")))

# COMMAND ----------

# MAGIC %md ## 2. Column-level profile (fill rate, non-numeric, negatives)

# COMMAND ----------

col_df = (spark.createDataFrame(column_rows, "level STRING, column_name STRING, row_count LONG, "
          "filled_cells LONG, fill_rate_pct DOUBLE, non_numeric_cells LONG, negative_cells LONG")
          .withColumn("profiled_at", F.lit(profiled_at)))
col_df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{ops}.profile_column_stats")
display(col_df.filter("non_numeric_cells > 0 OR negative_cells > 0").orderBy("level", "column_name"))

# COMMAND ----------

# MAGIC %md ## 3. Reference (crosswalk) files

# COMMAND ----------

ref_keys = {"CountyCrossWalk_Zillow.csv": "FIPS", "cities_crosswalk.csv": "Unique_City_ID"}
for file_name in REFERENCE_FILES:
    ref = read_raw_csv(file_name)
    key = [c for c in ref.columns if c.lstrip("﻿") == ref_keys[file_name]][0]
    r = ref.agg(F.count(F.lit(1)).alias("rows"), F.countDistinct(key).alias("distinct_keys")).first()
    nulls = {c: ref.filter(F.col(f"`{c}`").isNull()).count() for c in ref.columns}
    print(f"{file_name}: rows={r.rows}, distinct {ref_keys[file_name]}={r.distinct_keys}, "
          f"header={ref.columns}, nulls={ {k: v for k, v in nulls.items() if v} }")

# COMMAND ----------

# MAGIC %md ## 4. Data-quality findings and RegionName join coverage

# COMMAND ----------

cw = read_raw_csv("CountyCrossWalk_Zillow.csv")
cw = cw.toDF(*[c.lstrip("﻿") for c in cw.columns])          # strip the UTF-8 BOM
city_cw = read_raw_csv("cities_crosswalk.csv")

def regions(file_name):
    return read_raw_csv(file_name).select("RegionName").distinct()

def coverage(file_name, keys_df, transform=lambda c: c):
    r = regions(file_name).withColumn("k", transform(F.col("RegionName")))
    total = r.count()
    matched = r.join(keys_df.select(F.col("k")).distinct(), "k", "left_semi").count()
    return total, matched

findings = []
t, m = coverage("County_time_series.csv", cw.select(F.col("FIPS").alias("k")))
findings.append(("county", "FIPS join without padding", t, m))
t, m = coverage("County_time_series.csv", cw.select(F.col("FIPS").alias("k")), lambda c: F.lpad(c, 5, "0"))
findings.append(("county", "FIPS join after lpad(5,'0')", t, m))
t, m = coverage("Metro_time_series.csv", cw.select(F.col("CBSACode").alias("k")))
findings.append(("metro", "CBSACode join", t, m))
t, m = coverage("State_time_series.csv", cw.select(F.regexp_replace("StateName", " ", "").alias("k")))
findings.append(("state", "StateName (spaces removed) join", t, m))
t, m = coverage("City_time_series.csv", city_cw.select(F.col("Unique_City_ID").alias("k")))
findings.append(("city", "Unique_City_ID join", t, m))

nb = read_raw_csv("Neighborhood_time_series.csv")
bad_nb = nb.filter(~F.col("RegionName").rlike("^[0-9]+$"))
findings.append(("neighborhood", "RegionName not all digits (malformed, rows)", nb.count(), bad_nb.count()))

dq_df = (spark.createDataFrame(findings, "level STRING, check_name STRING, total LONG, matched_or_flagged LONG")
         .withColumn("profiled_at", F.lit(profiled_at)))
dq_df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{ops}.profile_dq_findings")
display(dq_df)
display(bad_nb.select("Date", "RegionName").limit(5))

# COMMAND ----------

# MAGIC %md ## 5. Metadata files (documentation only, not ingested)

# COMMAND ----------

display(spark.read.option("header", True).csv(f"{raw_path}/DataDictionary.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validation of this notebook's own results
# MAGIC These assertions match the offline profile in the project blueprint. If one fails,
# MAGIC the raw files differ from what was analysed and must be checked before chunking.

# COMMAND ----------

expected_rows = {"state": 13212, "metro": 211182, "county": 518791, "city": 3762566,
                 "neighborhood": 1695038, "zip": 4383885}
actual = {r.level: r.row_count for r in file_df.collect()}
assert actual == expected_rows, f"Row counts differ: {actual}"
assert all(r.duplicate_keys == 0 and r.bad_dates == 0 for r in file_df.collect()), "Key or date issues"
assert all(r.missing_columns is None and r.unexpected_columns is None for r in file_df.collect()), "Header mismatch"
assert bad_nb.count() == 302, "Expected 302 malformed neighborhood rows"
print(f"Profiling validated: {sum(actual.values()):,} rows across 6 files.")
