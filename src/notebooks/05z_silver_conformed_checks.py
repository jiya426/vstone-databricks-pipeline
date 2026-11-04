# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05z_silver_conformed_checks - checks for the Day 5 business-rule tables
# MAGIC | Table | Checks |
# MAGIC |---|---|
# MAGIC | metric_catalog | 96 metrics, unique names, every metric has a unit and precision, currency only on money units |
# MAGIC | region_metric_monthly | rows = non-NULL wide cells per level; unique (region_key, date_key, metric_name); no NULL value; every metric in catalogue; every region in reference; percents within 0-100 (tier share rescaled) |
# MAGIC | region_reference | unique region_key; all counties resolved; at least 98% of cities resolved |
# MAGIC | region_features_monthly | unique (region_key, date); rent yield plausible (0-50%); first 12 months have no YoY |

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from pyspark.sql import Window, functions as F
from vstone_framework.config import load_config
from vstone_framework.ops import ensure_ops_tables, write_check_results
from vstone_framework.quality import CheckResult, failed
from vstone_framework.runtime import get_context

cfg = load_config()
rules, ccfg = cfg["business_rules"], cfg["silver"]["conformed"]
ctx = get_context(dbutils)
ensure_ops_tables(spark, ctx.ops)
T = {k: f"{ctx.silver}.{ccfg[k]}" for k in ("metric_catalog_table", "long_table", "region_table", "features_table")}
results = []

def check(table, name, expected, actual, ok):
    results.append(CheckResult(table, name, str(expected), str(actual), "PASS" if ok else "FAIL"))

def dup_count(df, keys):
    return df.groupBy(*keys).count().filter("count > 1").count()

# COMMAND ----------

cat = spark.table(T["metric_catalog_table"])
n = cat.count()
check(T["metric_catalog_table"], "metric_count", 96, n, n == 96)
check(T["metric_catalog_table"], "unique_metric_name", 0, dup_count(cat, ["metric_name"]), dup_count(cat, ["metric_name"]) == 0)
missing = cat.filter("unit IS NULL OR decimal_places IS NULL").count()
check(T["metric_catalog_table"], "unit_and_precision_present", 0, missing, missing == 0)
wrong_ccy = cat.filter((F.col("unit").isin(*rules["currency_units"]) & F.col("currency_code").isNull())
                       | (~F.col("unit").isin(*rules["currency_units"]) & F.col("currency_code").isNotNull())).count()
check(T["metric_catalog_table"], "currency_only_on_money_units", 0, wrong_ccy, wrong_ccy == 0)

# COMMAND ----------

long = spark.table(T["long_table"])
latest = Window.partitionBy("level").orderBy(F.col("checked_at").desc())
recon = spark.table(f"{ctx.ops}.{ccfg['reconciliation_table']}").withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1")
for r in recon.collect():
    check(T["long_table"], f"reconciliation_{r.level}", r.expected_rows, r.actual_rows, bool(r.balanced))
dups = dup_count(long, ["region_key", "date_key", "metric_name"])
check(T["long_table"], "unique_region_date_metric", 0, dups, dups == 0)
nulls = long.filter("metric_value IS NULL OR region_key IS NULL OR date_key IS NULL").count()
check(T["long_table"], "no_null_values_or_keys", 0, nulls, nulls == 0)
orphan_metrics = long.select("metric_name").distinct().join(cat, "metric_name", "left_anti").count()
check(T["long_table"], "every_metric_in_catalog", 0, orphan_metrics, orphan_metrics == 0)
regions = spark.table(T["region_table"])
orphan_regions = long.select("region_key").distinct().join(regions, "region_key", "left_anti").count()
check(T["long_table"], "every_region_in_reference", 0, orphan_regions, orphan_regions == 0)
pct = long.filter("unit = 'PERCENT'").agg(F.min("metric_value").alias("lo"), F.max("metric_value").alias("hi")).first()
check(T["long_table"], "percent_range_after_normalisation", "[-10, 100]", f"[{pct.lo}, {pct.hi}]",
      pct.lo >= -10 and pct.hi <= 100)
tier = long.filter("metric_name LIKE 'inventory_tier_share%'").agg(F.max("metric_value")).first()[0]
check(T["long_table"], "tier_share_rescaled_to_percent", "> 1 and <= 100", tier, tier is not None and 1 < tier <= 100)
datekey = long.filter(F.col("date_key") != F.date_format("date", "yyyyMMdd").cast("int")).count()
check(T["long_table"], "date_key_matches_date", 0, datekey, datekey == 0)

# COMMAND ----------

dup_r = dup_count(regions, ["region_key"])
check(T["region_table"], "unique_region_key", 0, dup_r, dup_r == 0)
res = {r.level: (r.n, r.resolved) for r in regions.groupBy("level").agg(
    F.count("*").alias("n"), F.sum(F.col("is_resolved").cast("int")).alias("resolved")).collect()}
check(T["region_table"], "all_counties_resolved", res["county"][0], res["county"][1], res["county"][0] == res["county"][1])
check(T["region_table"], "all_states_resolved", res["state"][0], res["state"][1], res["state"][0] == res["state"][1])
city_share = res["city"][1] / res["city"][0]
check(T["region_table"], "city_resolution_at_least_98pct", ">= 0.98", round(city_share, 4), city_share >= 0.98)

# COMMAND ----------

feat = spark.table(T["features_table"])
dup_f = dup_count(feat, ["region_key", "date"])
check(T["features_table"], "unique_region_date", 0, dup_f, dup_f == 0)
y = feat.agg(F.min("gross_rent_yield_pct").alias("lo"), F.max("gross_rent_yield_pct").alias("hi")).first()
check(T["features_table"], "rent_yield_plausible_0_to_50", "(0, 50]", f"[{y.lo}, {y.hi}]",
      y.lo is not None and y.lo > 0 and y.hi <= 50)
first_dates = feat.groupBy("region_key").agg(F.min("date").alias("d0"))
early_yoy = feat.join(first_dates, "region_key").filter(
    (F.months_between("date", "d0") < 12) & F.col("zhvi_yoy_pct").isNotNull()).count()
check(T["features_table"], "no_yoy_in_first_12_months", 0, early_yoy, early_yoy == 0)

write_check_results(spark, ctx.ops, ctx.run_id, "silver", results)
display(spark.createDataFrame([r.__dict__ for r in results]))
bad = failed(results)
if bad:
    raise AssertionError("Conformed checks failed:\n" + "\n".join(
        f"{r.table_name} {r.check_name}: expected {r.expected}, got {r.actual}" for r in bad))
print(f"All {len(results)} conformed Silver checks passed.")
