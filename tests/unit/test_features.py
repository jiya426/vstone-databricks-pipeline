"""Unit tests for Silver features (growth rates with gap handling, rent yield, tier spread, listing premium)."""
from datetime import date

from pyspark.sql import functions as F

from vstone_framework.config import load_config
from vstone_framework.features import FEATURE_COLUMNS, compute_features

RULES = load_config()["business_rules"]


def month_ends(start_year, n):
    out, y, m = [], start_year, 1
    for _ in range(n):
        nxt = date(y + (m == 12), m % 12 + 1, 1)
        out.append(date.fromordinal(nxt.toordinal() - 1))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def test_growth_yield_spread_premium(spark):
    dates = month_ends(2010, 14)
    rows = [(d, "1003", 100000.0 + 1000 * i, 1000.0, 300000.0, 50000.0, 110000.0, None, "s", "f") for i, d in enumerate(dates)]
    df = spark.createDataFrame(rows, "date date, region_name string, zhvi_all_homes double, zri_all_homes double, "
                                     "zhvi_top_tier double, zhvi_bottom_tier double, median_listing_price_all_homes double, "
                                     "sale_counts double, source string, source_file string")
    out = df.transform(compute_features("county", RULES, "r")).orderBy("date").collect()
    assert out[0].zhvi_mom_pct is None and out[1].zhvi_mom_pct == round((101000 / 100000 - 1) * 100, 4)
    assert all(r.zhvi_yoy_pct is None for r in out[:12])
    assert out[12].zhvi_yoy_pct == round((112000 / 100000 - 1) * 100, 4)
    assert out[0].gross_rent_yield_pct == 12.0 and out[0].tier_spread_ratio == 6.0
    assert out[0].listing_premium_pct == 10.0 and out[0].region_key == "county:01003"
    assert list(df.transform(compute_features("county", RULES, "r")).columns) == FEATURE_COLUMNS


def test_gap_in_history_gives_no_change(spark):
    rows = [(date(2010, 1, 31), "a", 100.0), (date(2010, 3, 31), "a", 110.0)]   # February missing
    df = spark.createDataFrame(rows, "date date, region_name string, zhvi_all_homes double") \
              .withColumn("source", F.lit("s")).withColumn("source_file", F.lit("f"))
    out = df.transform(compute_features("zip", RULES, "r")).orderBy("date").collect()
    assert out[1].zhvi_mom_pct is None                       # 2 months apart -> not a month-over-month change
    assert out[0].zri_all_homes is None                      # missing columns become NULL, not errors
