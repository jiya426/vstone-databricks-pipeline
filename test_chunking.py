"""Unit tests for vstone_framework.chunking (Day 1 - Data Chunking)."""
from datetime import date

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

from vstone_framework.chunking import (CHUNK_TARGETS, chunk_output_path, invalid_xml_names,
                                       parse_cutoffs, plan_action, reconcile_counts, with_chunk_no)

CUTOFFS = parse_cutoffs(["2005-12-31", "2012-12-31", "2015-12-31"])


# ---------- parse_cutoffs ----------
def test_parse_cutoffs_valid():
    assert CUTOFFS == [date(2005, 12, 31), date(2012, 12, 31), date(2015, 12, 31)]


@pytest.mark.parametrize("values, message", [
    (["2012-12-31", "2005-12-31", "2015-12-31"], "ascending"),
    (["2005-12-31", "2005-12-31", "2015-12-31"], "ascending"),
    (["2005/12/31", "2012-12-31", "2015-12-31"], "format"),
    (["2005-12-31", "2012-12-31"], "Exactly 3"),
])
def test_parse_cutoffs_rejects_bad_input(values, message):
    with pytest.raises(ValueError, match=message):
        parse_cutoffs(values)


# ---------- with_chunk_no (used through DataFrame.transform) ----------
def test_with_chunk_no_assigns_boundaries_correctly(spark):
    src = spark.createDataFrame(
        [("1996-04-30", "a"), ("2005-12-31", "a"), ("2006-01-31", "a"), ("2012-12-31", "a"),
         ("2013-01-31", "a"), ("2015-12-31", "a"), ("2016-01-31", "a"), ("2017-12-31", "a"),
         ("not-a-date", "a")],
        "Date string, RegionName string")
    actual = src.transform(with_chunk_no(CUTOFFS))
    expected = spark.createDataFrame(
        [("1996-04-30", "a", 1), ("2005-12-31", "a", 1), ("2006-01-31", "a", 2), ("2012-12-31", "a", 2),
         ("2013-01-31", "a", 3), ("2015-12-31", "a", 3), ("2016-01-31", "a", 4), ("2017-12-31", "a", 4),
         ("not-a-date", "a", None)],
        "Date string, RegionName string, chunk_no int")
    assertDataFrameEqual(actual, expected)


def test_with_chunk_no_schema(spark):
    src = spark.createDataFrame([("2010-01-31", "10001")], "Date string, RegionName string")
    expected = StructType([StructField("Date", StringType()), StructField("RegionName", StringType()),
                           StructField("chunk_no", IntegerType())])
    assertSchemaEqual(src.transform(with_chunk_no(CUTOFFS)).schema, expected)


def test_every_row_lands_in_exactly_one_chunk(spark):
    months = spark.sql("SELECT date_format(last_day(add_months(DATE'1996-04-01', id)), 'yyyy-MM-dd') AS Date "
                       "FROM range(261)")
    counts = {r.chunk_no: r["count"] for r in months.transform(with_chunk_no(CUTOFFS)).groupBy("chunk_no").count().collect()}
    assert sum(counts.values()) == 261
    assert counts == {1: 117, 2: 84, 3: 36, 4: 24}


# ---------- helpers ----------
def test_chunk_targets_match_pdf():
    assert [(t.folder, t.file_format) for t in CHUNK_TARGETS.values()] == [
        ("csv_initial", "csv"), ("csv_incremental", "csv"), ("json", "json"), ("xml", "xml")]


def test_chunk_output_path():
    assert chunk_output_path("/Volumes/vstone/landing/chunks/", 3, "zip") == "/Volumes/vstone/landing/chunks/json/zip"
    with pytest.raises(ValueError):
        chunk_output_path("/x", 5, "zip")


def test_reconcile_counts():
    reconcile_counts(10, {1: 4, 2: 3, 3: 2, 4: 1})
    with pytest.raises(AssertionError, match="mismatch"):
        reconcile_counts(10, {1: 4, 2: 3, 3: 2, 4: 0})


def test_invalid_xml_names():
    real = ["Date", "RegionName", "ZHVI_AllHomes", "MedianListingPricePerSqft_1Bedroom", "Sale_Counts_Seas_Adj"]
    assert invalid_xml_names(real) == []
    assert invalid_xml_names(["Median Price", "1Bedroom", "xmlField"]) == ["Median Price", "1Bedroom", "xmlField"]


@pytest.mark.parametrize("status, exists, force, expected", [
    (None, False, False, "write"),        # first run
    ("SUCCESS", True, False, "skip"),     # re-run, nothing changed
    ("FAILED", True, False, "write"),     # resume after failure
    ("SUCCESS", False, False, "write"),   # output deleted by hand
    ("SUCCESS", True, True, "rewrite"),   # forced re-split
])
def test_plan_action_multiple_run_scenarios(status, exists, force, expected):
    assert plan_action(status, exists, force) == expected
