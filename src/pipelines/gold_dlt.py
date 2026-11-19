"""Lakeflow Declarative Pipeline (DLT) - Gold layer: star schema + aggregates.

Tables (names from conf/vstone_config.json -> gold.tables)
  dim_date                                Type 0 (static): generated calendar, never versioned
  dim_region                              Type 2: AUTO CDC from a snapshot keeps every version (__START_AT / __END_AT)
  dim_metric                              Type 1: AUTO CDC from a snapshot overwrites in place (one row per metric)
  dim_*_current                           one row per key, PRIMARY KEY
  fact_housing_metric_monthly             region x metric x month; FOREIGN KEYs to the 3 current dimensions
  agg_top10_regions_by_home_value, agg_monthly_market_trend, agg_state_sales_summary
  agg_market_churn, agg_market_churn_summary  (Day 7: churn-style metric)

Code parameterization: a pipeline has no widgets - values come from the `configuration` block in
resources/pipeline_gold_dlt.yml (vstone.catalog, vstone.silver_schema, vstone.gold_schema, vstone.framework_path).
All transformations live in vstone_framework.gold (unit tested); this file only declares the tables.
"""
import os
import sys

import dlt
from pyspark.sql import functions as F

_candidates = [spark.conf.get("vstone.framework_path", ""), os.path.join(os.getcwd(), ".."), os.getcwd()]
try:
    _candidates.insert(1, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
except NameError:
    pass
_src = next((p for p in _candidates if p and os.path.isdir(os.path.join(p, "vstone_framework"))), None)
if _src is None:
    raise ImportError(f"vstone_framework not found; tried {_candidates}. Check vstone.framework_path in the pipeline config.")
sys.path.insert(0, os.path.abspath(_src))
from vstone_framework.config import load_config  # noqa: E402
from vstone_framework.gold import (attach_region_sk, churn_summary, current_region_rows, dim_date_snapshot,  # noqa: E402
                                   dim_metric_snapshot, dim_region_snapshot, fact_frame, market_churn, market_trend,
                                   plain_rows, state_sales, top_regions)
from vstone_framework.gold_schema import (BUSINESS_KEYS, DIM_DATE, DIM_METRIC, KEYS, TABLE_COMMENTS,  # noqa: E402
                                          dlt_schema)

# ---- pipeline parameters (configuration block) ----
cfg = load_config()
g = cfg["gold"]
T = g["tables"]
catalog = spark.conf.get("vstone.catalog")
silver = spark.conf.get("vstone.silver_schema")
gold = spark.conf.get("vstone.gold_schema")
batch_id = spark.conf.get("pipelines.id", cfg["defaults"]["run_id"])
constraints = bool(g["declare_constraints"])
SILVER = {k: f"{catalog}.{silver}.{v}" for k, v in g["sources"].items()}
PROPS = {"quality": "gold", "project": cfg["project"]}


def schema(key):
    return dlt_schema(key, catalog, gold, constraints)


# ------------------------------------------------------------------ 1. snapshots of Silver (staging views)
@dlt.view(name="stg_dim_region", comment="Snapshot of silver.region_reference (business columns only)")
def stg_dim_region():
    return dim_region_snapshot(spark.read.table(SILVER["region"]))


@dlt.view(name="stg_dim_metric", comment="Snapshot of silver.metric_catalog (business columns only)")
def stg_dim_metric():
    return dim_metric_snapshot(spark.read.table(SILVER["metric"]))


# ------------------------------------------------------------------ 2. SCD Type 2: dim_region (AUTO CDC)
# Regions change over time (a county moves to another metro, a name is corrected), so the history is kept.
auto_cdc_from_snapshot = getattr(dlt, "create_auto_cdc_from_snapshot_flow", None) or dlt.apply_changes_from_snapshot

dlt.create_streaming_table(name=T["dim_region"], comment=TABLE_COMMENTS["dim_region"],
                           table_properties={**PROPS, "scd_type": "2"})
auto_cdc_from_snapshot(target=T["dim_region"], source="stg_dim_region",
                       keys=list(BUSINESS_KEYS["dim_region"]), stored_as_scd_type=2)


# ------------------------------------------------------------------ 3. Type 1: dim_metric (AUTO CDC, overwrite)
# A metric definition is corrected in place; no history is needed, so the target keeps one row per metric.
dlt.create_streaming_table(name=T["dim_metric"], comment=TABLE_COMMENTS["dim_metric"],
                           table_properties={**PROPS, "scd_type": "1"})
auto_cdc_from_snapshot(target=T["dim_metric"], source="stg_dim_metric",
                       keys=list(KEYS["dim_metric_current"][1]), stored_as_scd_type=1)


# ------------------------------------------------------------------ 4. dimension tables with PRIMARY KEYs
# dim_date is static (Type 0): the calendar is generated from Silver's date range, never versioned.
@dlt.table(name=T["dim_date_current"], comment=TABLE_COMMENTS["dim_date_current"],
           schema=schema("dim_date_current"), table_properties={**PROPS, "scd_type": "0"})
def dim_date_current():
    bounds = spark.read.table(SILVER["long"]).agg(F.min("date").alias("min_date"), F.max("date").alias("max_date"))
    return plain_rows(dim_date_snapshot(bounds), DIM_DATE)


@dlt.table(name=T["dim_region_current"], comment=TABLE_COMMENTS["dim_region_current"],
           schema=schema("dim_region_current"), table_properties=PROPS)
def dim_region_current():
    return current_region_rows(spark.read.table(T["dim_region"]))


@dlt.table(name=T["dim_metric_current"], comment=TABLE_COMMENTS["dim_metric_current"],
           schema=schema("dim_metric_current"), table_properties=PROPS)
def dim_metric_current():
    return plain_rows(spark.read.table(T["dim_metric"]), DIM_METRIC)


# ------------------------------------------------------------------ 4. fact (FOREIGN KEYs)
@dlt.table(name=T["fact"], comment=TABLE_COMMENTS["fact"], schema=schema("fact"),
           cluster_by=g["fact_cluster_by"], table_properties=PROPS)
@dlt.expect_all_or_fail({"keys_present": "region_key IS NOT NULL AND metric_key IS NOT NULL AND date_key IS NOT NULL",
                         "value_present": "metric_value IS NOT NULL"})
def fact_housing_metric_monthly():
    fact = fact_frame(spark.read.table(SILVER["long"]), SILVER["long"], batch_id)
    # Reading the dimensions makes the pipeline build them first (the FOREIGN KEYs need them). The semi-joins keep
    # only rows with a dimension row; the region version key comes from the current region rows; the Gold checks
    # prove none were dropped (fact rows = Silver rows).
    for dim_key, col in (("dim_metric_current", "metric_key"), ("dim_date_current", "date_key")):
        fact = fact.join(F.broadcast(spark.read.table(T[dim_key]).select(col)), col, "left_semi")
    return attach_region_sk(fact, spark.read.table(T["dim_region_current"]))


# ------------------------------------------------------------------ 5. aggregates
@dlt.table(name=T["agg_top_regions"], comment=TABLE_COMMENTS["agg_top_regions"], schema=schema("agg_top_regions"),
           table_properties=PROPS)
def agg_top_regions():
    return top_regions(spark.read.table(T["fact"]), spark.read.table(T["dim_region_current"]),
                       g["home_value_metric"], g["top_levels"], int(g["top_n"]), T["fact"], batch_id,
                       float(g["min_month_coverage"]))


@dlt.table(name=T["agg_market_trend"], comment=TABLE_COMMENTS["agg_market_trend"], schema=schema("agg_market_trend"),
           table_properties=PROPS)
def agg_market_trend():
    return market_trend(spark.read.table(SILVER["features"]), spark.read.table(T["dim_region_current"]),
                        g["trend_levels"], SILVER["features"], batch_id)


@dlt.table(name=T["agg_state_sales"], comment=TABLE_COMMENTS["agg_state_sales"], schema=schema("agg_state_sales"),
           table_properties=PROPS)
def agg_state_sales():
    return state_sales(spark.read.table(T["fact"]), spark.read.table(T["dim_region_current"]),
                       g["sales_metrics"], g["sales_level"], T["fact"], batch_id)


# ------------------------------------------------------------------ 6. churn-style metric (Day 7)
@dlt.table(name=T["agg_churn"], comment=TABLE_COMMENTS["agg_churn"], schema=schema("agg_churn"), table_properties=PROPS)
def agg_market_churn():
    c = g["churn"]
    return market_churn(spark.read.table(T["fact"]), spark.read.table(T["dim_region_current"]), c["metric"],
                        int(c["inactive_months"]), int(c["cooling_months"]), T["fact"], batch_id,
                        float(g["min_month_coverage"]))


@dlt.table(name=T["agg_churn_summary"], comment=TABLE_COMMENTS["agg_churn_summary"],
           schema=schema("agg_churn_summary"), table_properties=PROPS)
def agg_market_churn_summary():
    return churn_summary(spark.read.table(T["agg_churn"]), g["churn"]["unknown_state"], T["agg_churn"], batch_id)
