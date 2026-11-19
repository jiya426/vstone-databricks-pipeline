"""Unit tests for the Gold transformations, schemas and checks (vstone_framework.gold / gold_schema)."""
import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.testing import assertSchemaEqual

from vstone_framework.gold import (SCD_END, SCD_START, attach_region_sk, current_region_rows, dim_date_snapshot,
                                   dim_metric_snapshot, fact_frame, glossary_rows, gold_checks, market_trend, state_sales, top_regions)
from vstone_framework.gold_schema import (AGG_STATE_SALES, AGG_TOP_REGIONS, DIM_DATE, DIM_REGION_SOURCE, FACT,
                                          FACT_SOURCE, KEYS,
                                          column_names, dlt_schema, er_diagram_mermaid)

D = lambda s: dt.date.fromisoformat(s)


def regions(spark):
    rows = [("state:California", "state", "California", "California", "regional", "CA", "California"),
            ("state:Texas", "state", "Texas", "Texas", "regional", "TX", "Texas"),
            ("state:UnitedStates", "state", "UnitedStates", "United States", "national", None, None),
            ("zip:01001", "zip", "01001", "01001", "regional", None, None)]
    df = spark.createDataFrame(rows, "region_key string, level string, region_code string, region_name string, "
                                     "region_type string, state_code string, state_name string")
    for c in column_names(DIM_REGION_SOURCE):
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df


def fact(spark, rows):
    df = spark.createDataFrame([(r, m, k, Decimal(str(v)), "USD", "USD") for r, m, k, v in rows],
                                 "region_key string, metric_key string, date_key int, metric_value decimal(18,4), "
                                 "unit string, currency_code string")
    return df.withColumn("region_sk", F.xxhash64("region_key"))


# ---------------- dimensions
def test_calendar_has_every_month_end_without_gaps(spark):
    bounds = spark.createDataFrame([(D("1996-04-30"), D("2017-12-31"))], "min_date date, max_date date")
    cal = dim_date_snapshot(bounds)
    assert cal.columns == column_names(DIM_DATE)
    assert cal.count() == 261                                         # Apr 1996 .. Dec 2017
    dec = cal.filter("date_key = 20171231").first()
    assert (dec.date, dec.quarter_label, dec.month_name, dec.is_year_end) == (D("2017-12-31"), "2017-Q4", "December", True)
    feb = cal.filter("year = 2016 AND month = 2").first()
    assert feb.date == D("2016-02-29") and feb.date_key == 20160229   # leap year month-end


def test_metric_snapshot_renames_business_key(spark):
    cat = spark.createDataFrame([("zhvi_all_homes", "ZHVI_AllHomes", "ZHVI", "AllHomes", "USD", "USD", 1.0, 2,
                                  False, False, "Typical home value")],
                                "metric_name string, metric_code string, metric_family string, home_segment string, "
                                "unit string, currency_code string, scale_factor double, decimal_places int, "
                                "is_seasonally_adjusted boolean, is_msa_level boolean, definition string")
    assert dim_metric_snapshot(cat).first().metric_key == "zhvi_all_homes"


def test_current_rows_keep_only_the_open_scd2_version(spark):
    hist = regions(spark).withColumn("__START_AT", F.lit(1)).withColumn(SCD_END, F.lit(None).cast("int"))
    old = regions(spark).limit(1).withColumn("region_name", F.lit("Old name")) \
        .withColumn("__START_AT", F.lit(0)).withColumn(SCD_END, F.lit(1))
    cur = current_region_rows(hist.unionByName(old))
    assert cur.count() == 4 and cur.filter("region_name = 'Old name'").count() == 0


# ---------------- fact
def test_fact_has_schema_and_audit_columns(spark):
    long = spark.createDataFrame([("zip:01001", "zhvi_all_homes", 20171231, Decimal("194000"), "USD", "USD")],
                                 "region_key string, metric_name string, date_key int, metric_value decimal(18,4), "
                                 "unit string, currency_code string")
    out = fact_frame(long, "vstone.silver.region_metric_monthly", "pipe-1")
    assert out.columns == column_names(FACT_SOURCE)
    row = out.first()
    assert (row.metric_key, row.source, row.batch_id) == ("zhvi_all_homes", "gold_dlt", "pipe-1")


def test_region_sk_is_one_key_per_version_and_fact_takes_the_current_one(spark):
    base = regions(spark).limit(1)
    key = base.first().region_key
    old = base.withColumn(SCD_START, F.lit(1)).withColumn(SCD_END, F.lit(2))
    new = base.withColumn(SCD_START, F.lit(2)).withColumn(SCD_END, F.lit(None).cast("int"))
    cur = current_region_rows(old.unionByName(new))
    sk_now = spark.range(1).select(F.xxhash64(F.lit(key), F.lit(2))).first()[0]
    assert cur.count() == 1 and cur.first().region_sk == sk_now
    long = spark.createDataFrame([(key, "zhvi_all_homes", 20171231, Decimal("1"), "USD", "USD")],
                                 "region_key string, metric_name string, date_key int, metric_value decimal(18,4), "
                                 "unit string, currency_code string")
    out = attach_region_sk(fact_frame(long, "src", "b"), cur)
    assert out.columns == column_names(FACT) and out.first().region_sk == sk_now


# ---------------- aggregates
def test_top_regions_latest_month_rank_and_yoy(spark):
    f = fact(spark, [("state:California", "zhvi_all_homes", 20171231, 500000),
                     ("state:California", "zhvi_all_homes", 20161231, 400000),
                     ("state:Texas", "zhvi_all_homes", 20171231, 200000),
                     ("state:UnitedStates", "zhvi_all_homes", 20171231, 999999),   # national row excluded
                     ("zip:01001", "zhvi_all_homes", 20171231, 194000),
                     ("state:Texas", "zri_all_homes", 20171231, 9999999)])          # other metric ignored
    top = top_regions(f, regions(spark), "zhvi_all_homes", ["state", "zip"], 1, "fact", "b")
    assert top.columns == column_names(AGG_TOP_REGIONS)
    rows = {r.level: r for r in top.collect()}
    assert set(rows) == {"state", "zip"}
    ca = rows["state"]
    assert (ca.region_key, ca.rank_in_level, ca.date, ca.home_value_yoy_pct) == ("state:California", 1, D("2017-12-31"), 25.0)


def test_state_sales_per_state_and_year(spark):
    m = {"sales_count": "sale_counts", "sale_price": "sale_prices", "home_value": "zhvi_all_homes",
         "listing_price": "median_listing_price_all_homes"}
    f = fact(spark, [("state:Texas", "sale_counts", 20170131, 100), ("state:Texas", "sale_counts", 20170228, 50),
                     ("state:Texas", "sale_prices", 20170131, 200000), ("state:Texas", "sale_prices", 20170228, 220000),
                     ("state:UnitedStates", "sale_counts", 20170131, 9999)])
    out = state_sales(f, regions(spark), m, "state", "fact", "b")
    assert out.columns == column_names(AGG_STATE_SALES)
    tx = out.collect()
    assert len(tx) == 1
    assert (tx[0].state_code, tx[0].year, tx[0].months_reported, tx[0].homes_sold, tx[0].avg_sale_price_usd) == \
        ("TX", 2017, 2, Decimal("150.0000"), Decimal("210000.0000"))


def test_market_trend_renames_to_business_names(spark):
    feats = spark.createDataFrame([("state", "California", "state:California", D("2017-12-31"), 20171231, 2017, 4, 12,
                                    D("2017-12-01"), 500000.0, 2800.0, None, None, 550000.0, 30000.0,
                                    0.5, 7.0, 3.0, 6.72, None, 10.0)],
                                  "level string, region_code string, region_key string, date date, date_key int, "
                                  "year int, quarter int, month int, month_start_date date, zhvi_all_homes double, "
                                  "zri_all_homes double, zhvi_top_tier double, zhvi_bottom_tier double, "
                                  "median_listing_price_all_homes double, sale_counts double, zhvi_mom_pct double, "
                                  "zhvi_yoy_pct double, zri_yoy_pct double, gross_rent_yield_pct double, "
                                  "tier_spread_ratio double, listing_premium_pct double")
    out = market_trend(feats, regions(spark), ["state"], "features", "b").first()
    assert (out.region_name, out.state_code, out.home_value_usd, out.monthly_rent_usd, out.homes_sold) == \
        ("California", "CA", 500000.0, 2800.0, 30000.0)


# ---------------- schema strings, ER diagram, glossary
def test_dlt_schema_declares_comments_primary_and_foreign_keys():
    s = dlt_schema("fact", "vstone", "gold")
    assert "region_sk BIGINT NOT NULL COMMENT" in s
    assert "CONSTRAINT fact_housing_metric_monthly_pk PRIMARY KEY (region_sk, metric_key, date_key)" in s
    assert "FOREIGN KEY (region_sk) REFERENCES vstone.gold.dim_region_current(region_sk)" in s
    assert "FOREIGN KEY (date_key) REFERENCES vstone.gold.dim_date_current(date_key)" in s
    plain = dlt_schema("fact", "vstone", "gold", with_constraints=False)
    assert "CONSTRAINT" not in plain and "NOT NULL" not in plain


def test_every_fact_foreign_key_points_to_a_dimension_primary_key():
    for col, (ref, ref_col) in KEYS["fact"][2].items():
        assert KEYS[ref][1] == (ref_col,)


def test_er_diagram_is_generated_from_the_keys():
    er = er_diagram_mermaid()
    assert er.startswith("erDiagram")
    assert "dim_region_current ||--o{ fact_housing_metric_monthly : region_sk" in er


def test_glossary_covers_metrics_features_levels_and_keys():
    cat = [{"metric_name": "zhvi_all_homes", "metric_code": "ZHVI_AllHomes", "definition": "Typical home value",
            "unit": "USD"}]
    rows = glossary_rows(cat, {"zhvi_yoy_pct": "YoY change"}, {"zip": "5-digit ZIP"}, "fact", "trend", "dim_region")
    cats = {r[2] for r in rows}
    assert cats == {"Metric", "Feature", "Geography level", "Key"}
    assert ("zhvi_all_homes", "ZHVI_AllHomes", "Metric", "Typical home value", "USD", "fact.metric_key",
            "Zillow data dictionary") in rows


# ---------------- checks
def test_gold_checks_catch_orphans_and_scd2_errors(spark):
    reg = regions(spark).withColumn("region_sk", F.xxhash64("region_key"))
    f = fact(spark, [("state:Texas", "zhvi_all_homes", 20171231, 1), ("zip:99999", "zhvi_all_homes", 20171231, 2)])
    dims = {"dim_region_current": reg,
            "dim_metric_current": spark.createDataFrame([("zhvi_all_homes",)], "metric_key string"),
            "dim_date_current": spark.createDataFrame([(20171231,)], "date_key int")}
    hist = {"dim_region": reg.withColumn(SCD_END, F.lit(None).cast("int"))
                             .unionByName(reg.limit(1).withColumn(SCD_END, F.lit(None).cast("int")))}
    pks = {"fact": ["region_sk", "metric_key", "date_key"], "dim_region_current": ["region_sk"],
           "dim_metric_current": ["metric_key"], "dim_date_current": ["date_key"]}
    fks = {"region_sk": "dim_region_current", "metric_key": "dim_metric_current", "date_key": "dim_date_current"}
    top = spark.createDataFrame([("state", 1)], "level string, rank_in_level int")
    res = gold_checks(f, 2, dims, hist, pks, fks, top, 10, ["state"], top, top, ["state", "zip"], "fact",
                      "dim_region_current")
    failed = {(r.table_name, r.check_name) for r in res if r.status == "FAIL"}
    assert failed == {("fact", "region_sk_has_dimension_row"), ("dim_region", "scd2_one_current_version_per_key"),
                      ("fact", "all_levels_reach_gold")}
