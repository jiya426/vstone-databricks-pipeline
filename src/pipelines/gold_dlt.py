"""Lakeflow Declarative Pipeline (DLT) - Gold layer: star schema + aggregates.

Tables (names from conf/vstone_config.json -> gold.tables)
  dim_date, dim_region, dim_metric        SCD Type 2: AUTO CDC from a snapshot of Silver keeps every version
                                          (__START_AT / __END_AT added by the pipeline)
  dim_*_current                           current version of each key; PRIMARY KEY
  fact_housing_metric_monthly             region x metric x month; FOREIGN KEYs to the 3 current dimensions
  agg_top10_regions_by_home_value, agg_monthly_market_trend, agg_state_sales_summary

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
from vstone_framework.gold import (current_rows, dim_date_snapshot, dim_metric_snapshot,  # noqa: E402
                                   dim_region_snapshot, fact_frame, market_trend, state_sales, top_regions)
from vstone_framework.gold_schema import (DIM_DATE, DIM_METRIC, DIM_REGION, KEYS, TABLE_COMMENTS,  # noqa: E402
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
@dlt.view(name="stg_dim_date", comment="Calendar of month-ends covering every month in Silver")
def stg_dim_date():
    bounds = spark.read.table(SILVER["long"]).agg(F.min("date").alias("min_date"), F.max("date").alias("max_date"))
    return dim_date_snapshot(bounds)


@dlt.view(name="stg_dim_region", comment="Snapshot of silver.region_reference (business columns only)")
def stg_dim_region():
    return dim_region_snapshot(spark.read.table(SILVER["region"]))


@dlt.view(name="stg_dim_metric", comment="Snapshot of silver.metric_catalog (business columns only)")
def stg_dim_metric():
    return dim_metric_snapshot(spark.read.table(SILVER["metric"]))


# ------------------------------------------------------------------ 2. SCD Type 2 dimensions (AUTO CDC)
auto_cdc_from_snapshot = getattr(dlt, "create_auto_cdc_from_snapshot_flow", None) or dlt.apply_changes_from_snapshot

for dim, staging, current_key in (("dim_date", "stg_dim_date", "dim_date_current"),
                                  ("dim_region", "stg_dim_region", "dim_region_current"),
                                  ("dim_metric", "stg_dim_metric", "dim_metric_current")):
    dlt.create_streaming_table(name=T[dim], comment=TABLE_COMMENTS[dim],
                               table_properties={**PROPS, "scd_type": "2"})
    auto_cdc_from_snapshot(target=T[dim], source=staging, keys=list(KEYS[current_key][1]), stored_as_scd_type=2)


# ------------------------------------------------------------------ 3. current dimensions (PRIMARY KEYs)
def declare_current(key, history_key, columns):
    @dlt.table(name=T[key], comment=TABLE_COMMENTS[key], schema=schema(key), table_properties=PROPS)
    def _current():
        return current_rows(spark.read.table(T[history_key]), columns)


declare_current("dim_date_current", "dim_date", DIM_DATE)
declare_current("dim_region_current", "dim_region", DIM_REGION)
declare_current("dim_metric_current", "dim_metric", DIM_METRIC)


# ------------------------------------------------------------------ 4. fact (FOREIGN KEYs)
@dlt.table(name=T["fact"], comment=TABLE_COMMENTS["fact"], schema=schema("fact"),
           cluster_by=g["fact_cluster_by"], table_properties=PROPS)
@dlt.expect_all_or_fail({"keys_present": "region_key IS NOT NULL AND metric_key IS NOT NULL AND date_key IS NOT NULL",
                         "value_present": "metric_value IS NOT NULL"})
def fact_housing_metric_monthly():
    fact = fact_frame(spark.read.table(SILVER["long"]), SILVER["long"], batch_id)
    # Reading the dimensions makes the pipeline build them first (the FOREIGN KEYs need them). The semi-joins keep
    # only rows with a dimension row; the Gold checks prove none were dropped (fact rows = Silver rows).
    for dim_key, col in (("dim_region_current", "region_key"), ("dim_metric_current", "metric_key"),
                         ("dim_date_current", "date_key")):
        fact = fact.join(F.broadcast(spark.read.table(T[dim_key]).select(col)), col, "left_semi")
    return fact


# ------------------------------------------------------------------ 5. aggregates
@dlt.table(name=T["agg_top_regions"], comment=TABLE_COMMENTS["agg_top_regions"], schema=schema("agg_top_regions"),
           table_properties=PROPS)
def agg_top_regions():
    return top_regions(spark.read.table(T["fact"]), spark.read.table(T["dim_region_current"]),
                       g["home_value_metric"], g["top_levels"], int(g["top_n"]), T["fact"], batch_id)


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
