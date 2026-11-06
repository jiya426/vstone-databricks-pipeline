"""Silver -> Gold on the REAL data samples (all 6 levels + both crosswalks):
dimensions, fact, aggregates and the production Gold checks (reconciliation, keys, referential integrity, SCD2)."""
import pytest
from pyspark.sql import functions as F

from vstone_framework.business_rules import metric_catalog
from vstone_framework.config import load_config
from vstone_framework.features import compute_features
from vstone_framework.gold import (SCD_END, dim_date_snapshot, dim_metric_snapshot, dim_region_snapshot, fact_frame,
                                   gold_checks, market_trend, state_sales, top_regions)
from vstone_framework.gold_schema import KEYS
from vstone_framework.headers import to_snake_case
from vstone_framework.regions import build_region_reference
from vstone_framework.silver import clean_reference

from .test_conformed_on_real_sample import DICT, RULES, long_for, materialized_silver
from .test_silver_on_real_sample import FIELDS, FIX

CFG = load_config()
G, T = CFG["gold"], CFG["gold"]["tables"]


def crosswalk(spark, name, file):
    df = spark.read.option("header", True).csv(f"{FIX}/{file}")
    mapping = {c: to_snake_case(c) for c in df.columns}
    df = (df.withColumn("source", F.lit("copy_into_csv")).withColumn("source_file", F.lit(file))
            .withColumn("load_dt", F.current_timestamp()))
    return df.transform(clean_reference(mapping, CFG["silver"]["reference_rules"][name], "t"))


@pytest.fixture(scope="module")
def gold(spark):
    long = None
    for level in CFG["levels"]:
        _, _, part = long_for(spark, level)
        long = part if long is None else long.unionByName(part)
    long = spark.createDataFrame(long.collect(), long.schema)                     # materialise like a Silver table
    codes = {c for v in FIELDS.values() for c in v if c not in ("Date", "RegionName")}
    cat = spark.createDataFrame(metric_catalog(codes, DICT, RULES))
    regions = build_region_reference(long.select("level", "region_code").distinct(),
                                     crosswalk(spark, "county_crosswalk", "county_crosswalk_sample.csv"),
                                     crosswalk(spark, "city_crosswalk", "city_crosswalk_sample.csv"), RULES)
    feats = materialized_silver(spark, "state").transform(compute_features("state", RULES, "t"))

    dims = {"dim_region": dim_region_snapshot(regions), "dim_metric": dim_metric_snapshot(cat),
            "dim_date": dim_date_snapshot(long.agg(F.min("date").alias("min_date"), F.max("date").alias("max_date")))}
    dims = {k: spark.createDataFrame(v.collect(), v.schema) for k, v in dims.items()}
    current = {T[f"{k}_current"]: v for k, v in dims.items()}                      # first load: every row current
    history = {T[k]: v.withColumn(SCD_END, F.lit(None).cast("timestamp")) for k, v in dims.items()}
    fact = fact_frame(long, "silver.region_metric_monthly", "t")
    reg = current[T["dim_region_current"]]
    return {"long": long, "fact": fact, "current": current, "history": history, "regions": reg,
            "top": top_regions(fact, reg, G["home_value_metric"], G["top_levels"], int(G["top_n"]), "f", "t"),
            "trend": market_trend(feats, reg, G["trend_levels"], "features", "t"),
            "sales": state_sales(fact, reg, G["sales_metrics"], G["sales_level"], "f", "t")}


def test_production_gold_checks_pass_on_real_data(gold):
    pks = {T[k]: list(KEYS[k][1]) for k in ("fact", "dim_date_current", "dim_region_current", "dim_metric_current")}
    fks = {col: T[ref] for col, (ref, _) in KEYS["fact"][2].items()}
    results = gold_checks(gold["fact"], gold["long"].count(), gold["current"], gold["history"], pks, fks, gold["top"],
                          int(G["top_n"]), G["top_levels"], gold["trend"], gold["sales"], CFG["levels"], T["fact"],
                          T["dim_region_current"])
    assert [r for r in results if r.status == "FAIL"] == []
    assert len(results) >= 15


def test_crosswalk_names_reach_gold(gold):
    reg = {r.region_key: r for r in gold["regions"].collect()}
    assert reg["county:01003"].county_name == "Baldwin" and reg["county:01003"].state_code == "AL"
    assert reg["city:aberdeenharfordmd"].city_name == "Aberdeen"


def test_known_value_flows_from_raw_to_fact(gold):
    v = gold["fact"].filter("region_key = 'zip:01001' AND metric_key = 'zhvi_all_homes' AND date_key = 20000131").first()
    assert str(v.metric_value) == "113500.0000" and v.unit == "USD"


def test_aggregates_answer_the_business_questions(gold):
    top = gold["top"].filter("level = 'state'").orderBy("rank_in_level").collect()
    assert 1 <= len(top) <= 10 and top[0].rank_in_level == 1
    assert all(a.home_value_usd >= b.home_value_usd for a, b in zip(top, top[1:]))
    trend = gold["trend"]
    assert trend.count() > 0 and trend.filter("region_name IS NULL OR home_value_usd IS NULL").count() == 0
    al = gold["sales"].filter("state_code = 'AL' AND year = 2016").first()
    assert al is not None and al.homes_sold > 0 and al.months_reported == 12
