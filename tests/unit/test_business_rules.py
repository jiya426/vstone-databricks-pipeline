"""Unit tests for Day 5 business rules: metric catalogue, value normalisation, dates, region codes, unpivot."""
import json
import os
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from vstone_framework.business_rules import (classify_metric, metric_catalog, non_null_cells, normalized_value,
                                             region_code, unpivot_metrics, with_date_attributes)
from vstone_framework.config import load_config
from vstone_framework.metadata import load_data_dictionary

FIX = os.path.join(os.path.dirname(__file__), "..", "fixtures")
RULES = load_config()["business_rules"]
DICT = load_data_dictionary(os.path.join(FIX, "DataDictionary.csv"))


@pytest.mark.parametrize("code, unit, ccy, scale, dp, sa", [
    ("ZHVI_AllHomes", "USD", "USD", 1.0, 2, True),
    ("ZRI_AllHomes", "USD_PER_MONTH", "USD", 1.0, 2, True),
    ("MedianListingPricePerSqft_3Bedroom", "USD_PER_SQFT", "USD", 1.0, 4, False),
    ("InventoryTierShare_TopTier_AllHomes", "PERCENT", None, 100.0, 4, False),
    ("PctOfListingsWithPriceReductionsSeasAdj_AllHomes", "PERCENT", None, 1.0, 4, True),
    ("Sale_Counts_Seas_Adj", "COUNT", None, 1.0, 0, True),
    ("DaysOnZillow_AllHomes", "DAYS", None, 1.0, 1, False),
    ("PriceToRentRatio_AllHomes", "RATIO", None, 1.0, 4, False),
])
def test_classify_metric(code, unit, ccy, scale, dp, sa):
    m = classify_metric(code, DICT, RULES)
    assert (m["unit"], m["currency_code"], m["scale_factor"], m["decimal_places"], m["is_seasonally_adjusted"]) == \
           (unit, ccy, scale, dp, sa)
    assert m["definition"] and m["metric_name"] == m["metric_name"].lower()


def test_catalog_covers_all_96_real_metrics():
    with open(os.path.join(FIX, "fields_per_level.json")) as fh:
        codes = {c for v in json.load(fh).values() for c in v if c not in ("Date", "RegionName")}
    cat = metric_catalog(codes, DICT, RULES)
    assert len(cat) == 96 and len({m["metric_name"] for m in cat}) == 96
    assert {m["metric_name"] for m in cat if m["is_msa_level"]} == {"sale_counts_msa", "sale_counts_seas_adj_msa", "sale_prices_msa"}


def test_normalized_value_rounding_scaling_and_currency(spark):
    df = spark.createDataFrame([(113500.456, "USD", 1.0), (0.1372549, "PERCENT", 100.0), (183.6, "COUNT", 1.0),
                                (0.4589, "USD_PER_SQFT_PER_MONTH", 1.0), (27.04, "DAYS", 1.0)],
                               "raw double, unit string, scale double")
    out = df.select(normalized_value(F.col("raw"), F.col("unit"), F.col("scale"), RULES).alias("v"))
    assert [r.v for r in out.collect()] == [Decimal("113500.4600"), Decimal("13.7255"), Decimal("184.0000"),
                                            Decimal("0.4589"), Decimal("27.0000")]


def test_region_code_pads_county_only(spark):
    df = spark.createDataFrame([("1003",)], "r string")
    assert df.select(region_code("county", F.col("r"), RULES)).first()[0] == "01003"
    assert df.select(region_code("zip", F.col("r"), RULES)).first()[0] == "1003"


def test_date_attributes(spark):
    df = spark.createDataFrame([(date(2017, 12, 31),)], "date date").transform(with_date_attributes())
    expected = spark.createDataFrame([(date(2017, 12, 31), 20171231, 2017, 4, 12, date(2017, 12, 1))],
                                     "date date, date_key int, year int, quarter int, month int, month_start_date date")
    assertDataFrameEqual(df, expected)


def test_unpivot_drops_nulls_and_counts_match(spark):
    wide = spark.createDataFrame([(date(2010, 1, 31), "1003", 100.0, None, "s", "f"),
                                  (date(2010, 2, 28), "1003", 101.0, 5.0, "s", "f")],
                                 "date date, region_name string, zhvi_all_homes double, sale_counts double, source string, source_file string")
    long = wide.transform(unpivot_metrics("county", ["zhvi_all_homes", "sale_counts"], RULES))
    assert long.count() == 3 == non_null_cells(wide, ["zhvi_all_homes", "sale_counts"])
    assert {r.region_key for r in long.collect()} == {"county:01003"}
