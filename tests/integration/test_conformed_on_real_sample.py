"""Silver (wide) -> conformed long table and features on the real-data fixtures."""
from decimal import Decimal

from vstone_framework.business_rules import apply_catalog, metric_catalog, non_null_cells, unpivot_metrics
from vstone_framework.config import load_config
from vstone_framework.features import compute_features
from vstone_framework.headers import to_snake_case
from vstone_framework.metadata import load_data_dictionary

from .test_silver_on_real_sample import FIELDS, FIX, run_silver

CFG = load_config()
RULES = CFG["business_rules"]
DICT = load_data_dictionary(f"{FIX}/DataDictionary.csv")


def catalog_df(spark):
    codes = {c for v in FIELDS.values() for c in v if c not in ("Date", "RegionName")}
    rows = metric_catalog(codes, DICT, RULES)
    return spark.createDataFrame([(r["metric_name"], r["unit"], r["currency_code"], r["scale_factor"]) for r in rows],
                                 "metric_name string, unit string, currency_code string, scale_factor double")


def materialized_silver(spark, level):
    """Production reads Silver from a stored Delta table; materialise it here too so the Day 5 plan starts
    from stored data instead of re-planning the whole Bronze -> Silver chain."""
    wide, _, _, _ = run_silver(spark, level)
    return spark.createDataFrame(wide.collect(), wide.schema)       # small sample: rows -> new DataFrame


def long_for(spark, level):
    wide = materialized_silver(spark, level)
    metrics = [to_snake_case(c) for c in FIELDS[level] if c not in ("Date", "RegionName")]
    long = wide.transform(unpivot_metrics(level, metrics, RULES)).transform(apply_catalog(catalog_df(spark), RULES, "t"))
    return wide, metrics, long


def test_long_rows_reconcile_and_values_normalised(spark):
    wide, metrics, long = long_for(spark, "zip")
    assert long.count() == non_null_cells(wide, metrics)
    assert long.filter("unit IS NULL OR metric_value IS NULL").count() == 0
    v = long.filter("region_code = '01001' AND date_key = 20000131 AND metric_name = 'zhvi_all_homes'").first()
    assert v.metric_value == Decimal("113500.0000") and v.currency_code == "USD" and v.unit == "USD"


def test_county_codes_standardised(spark):
    _, _, long = long_for(spark, "county")
    assert {r.region_code for r in long.select("region_code").distinct().collect()} == {"01003", "10001"}


def test_metro_tier_share_rescaled_to_percent(spark):
    _, _, long = long_for(spark, "metro")
    hi = long.filter("metric_name LIKE 'inventory_tier_share%'").agg({"metric_value": "max"}).first()[0]
    assert hi is not None and Decimal("1") < hi <= Decimal("100")


def test_features_on_real_state_sample(spark):
    wide = materialized_silver(spark, "state")
    feat = wide.transform(compute_features("state", RULES, "t"))
    al = feat.filter("region_key = 'state:Alabama' AND date_key = 20170131").first()
    assert al.zhvi_yoy_pct is not None and -20 < al.zhvi_yoy_pct < 20
    assert al.gross_rent_yield_pct is None or 0 < al.gross_rent_yield_pct < 50
