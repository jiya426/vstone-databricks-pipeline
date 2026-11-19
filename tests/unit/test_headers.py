"""Unit tests for header standardisation: Python function, Python UDF and pandas UDF."""
import pytest
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

from vstone_framework.headers import (header_mapping, snake_case_pandas_udf, snake_case_udf, standardize_headers,
                                      to_snake_case, validate_mapping)

CASES = {
    "ZHVI_AllHomes": "zhvi_all_homes",
    "MedianListingPricePerSqft_1Bedroom": "median_listing_price_per_sqft_1_bedroom",
    "ZHVI_1bedroom": "zhvi_1_bedroom",
    "MedianRentalPrice_MultiFamilyResidence5PlusUnits": "median_rental_price_multi_family_residence_5_plus_units",
    "﻿CountyName": "county_name",
    "CBSACode": "cbsa_code",
    "ZHVIPerSqft_AllHomes": "zhvi_per_sqft_all_homes",
    "Sale_Counts_Seas_Adj_Msa": "sale_counts_seas_adj_msa",
    "Unique_City_ID": "unique_city_id",
    "RegionName": "region_name",
    "  Median Price-USD ": "median_price_usd",
    "5YearChange": "c_5_year_change",
}


@pytest.mark.parametrize("raw, expected", CASES.items())
def test_to_snake_case(raw, expected):
    assert to_snake_case(raw) == expected


def test_python_and_pandas_udf_agree_with_function(spark):
    df = spark.createDataFrame([(k,) for k in CASES], "original string")
    py = df.select("original", snake_case_udf("original").alias("std"))
    pd_ = df.select("original", snake_case_pandas_udf("original").alias("std"))
    expected = spark.createDataFrame(list(CASES.items()), "original string, std string")
    assertDataFrameEqual(py, expected)
    assertDataFrameEqual(pd_, expected)


@pytest.mark.parametrize("method", ["pandas", "python"])
def test_header_mapping_and_rename(spark, method):
    mapping = header_mapping(spark, ["Date", "RegionName", "ZHVI_AllHomes"], method=method)
    assert mapping == {"Date": "date", "RegionName": "region_name", "ZHVI_AllHomes": "zhvi_all_homes"}
    df = spark.createDataFrame([("2010-01-31", "10001", "1.5")], "Date string, RegionName string, ZHVI_AllHomes string")
    out = df.transform(standardize_headers(mapping))
    assertSchemaEqual(out.schema, spark.createDataFrame([], "date string, region_name string, zhvi_all_homes string").schema)
    assertDataFrameEqual(out, spark.createDataFrame([("2010-01-31", "10001", "1.5")],
                                                    "date string, region_name string, zhvi_all_homes string"))


def test_collisions_are_rejected():
    with pytest.raises(ValueError, match="collision"):
        validate_mapping({"ZHVI_AllHomes": "zhvi_all_homes", "ZHVIAllHomes": "zhvi_all_homes"})
    with pytest.raises(ValueError, match="Invalid"):
        validate_mapping({"x": "1abc"})
