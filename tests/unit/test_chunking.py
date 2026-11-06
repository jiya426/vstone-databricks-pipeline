"""Unit tests for vstone_framework.chunking (Day 1 - Data Chunking)."""
from datetime import date

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

from vstone_framework.chunking import (CHUNK_TARGETS, LEVEL_COL, chunk_file, chunk_folder, combine_levels,
                                       combined_columns, invalid_xml_names, legacy_layout, parse_cutoffs,
                                       plan_action, reconcile_counts, reconcile_levels, staging_folder,
                                       tag_level, with_chunk_no)

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
def test_exactly_four_files_two_csv_one_json_one_xml():
    assert [(t.folder, t.file_format, t.ingestion) for t in CHUNK_TARGETS.values()] == [
        ("chunk1_csv", "csv", "copy_into"), ("chunk2_csv", "csv", "dlt"),
        ("chunk3_json", "json", "autoloader"), ("chunk4_xml", "xml", "pyspark_xml")]
    names = [t.file_name for t in CHUNK_TARGETS.values()]
    assert len(set(names)) == 4
    assert all(t.file_name.endswith("." + t.file_format) for t in CHUNK_TARGETS.values())


def test_chunk_paths():
    root = "/Volumes/vstone/landing/chunks/"
    assert chunk_folder(root, 3) == "/Volumes/vstone/landing/chunks/chunk3_json"
    assert chunk_file(root, 3) == "/Volumes/vstone/landing/chunks/chunk3_json/zillow_time_series_chunk3.json"
    assert staging_folder(root, 3) == "/Volumes/vstone/landing/chunks/_staging/chunk3_json"
    with pytest.raises(ValueError):
        chunk_folder("/x", 5)


# ---------- combining the 6 levels into one dataset ----------
FIELDS = {"state": ["Date", "RegionName", "ZHVI_AllHomes", "ZRI_AllHomes"],
          "zip": ["RegionName", "Date", "ZHVI_AllHomes", "MedianListingPrice_AllHomes"]}


def test_combined_columns_are_keys_level_then_each_metric_once():
    assert combined_columns(FIELDS, ["state", "zip"]) == [
        "Date", "RegionName", LEVEL_COL, "ZHVI_AllHomes", "ZRI_AllHomes", "MedianListingPrice_AllHomes"]


def test_combine_levels_tags_rows_and_fills_missing_metrics_with_null(spark):
    cols = combined_columns(FIELDS, ["state", "zip"])
    state = spark.createDataFrame([("2017-12-31", "California", "500000", "2800")], "Date string, RegionName string, "
                                  "ZHVI_AllHomes string, ZRI_AllHomes string")
    zips = spark.createDataFrame([("01001", "2017-12-31", "200000", "250000")], "RegionName string, Date string, "
                                 "ZHVI_AllHomes string, MedianListingPrice_AllHomes string")
    out = combine_levels({"state": state, "zip": zips}, cols)
    assert out.columns == cols
    expected = spark.createDataFrame(
        [("2017-12-31", "California", "state", "500000", "2800", None),
         ("2017-12-31", "01001", "zip", "200000", None, "250000")], ", ".join(f"{c} string" for c in cols))
    assertDataFrameEqual(out, expected)
    assert out.count() == state.count() + zips.count()            # nothing lost, nothing added


def test_tag_level_keeps_values_in_the_right_columns_whatever_the_input_order(spark):
    cols = combined_columns(FIELDS, ["state", "zip"])
    zips = spark.createDataFrame([("01001", "2017-12-31", "200000", "250000")], "RegionName string, Date string, "
                                 "ZHVI_AllHomes string, MedianListingPrice_AllHomes string")
    row = zips.transform(tag_level("zip", cols)).first()
    assert (row.Date, row.RegionName, row[LEVEL_COL], row.MedianListingPrice_AllHomes) == (
        "2017-12-31", "01001", "zip", "250000")


def test_reconcile_levels():
    expected = {"state": 3, "zip": 5}
    assert reconcile_levels(expected, {1: {"state": 1, "zip": 2}, 2: {"state": 2, "zip": 3}}) == []
    errors = reconcile_levels(expected, {1: {"state": 1, "zip": 2}, 2: {"zip": 3, "moon": 1}})
    assert any(e.startswith("state") for e in errors) and any("moon" in e for e in errors)


def test_legacy_layout_lists_the_24_old_tables_and_4_old_folders():
    legacy = legacy_layout()
    assert len(legacy["bronze_tables"]) == 24 and "zip_ts_csv_dlt" in legacy["bronze_tables"]
    assert legacy["chunk_folders"] == ["csv_initial", "csv_incremental", "json", "xml"]
    new_folders = {t.folder for t in CHUNK_TARGETS.values()}
    assert not new_folders & set(legacy["chunk_folders"])          # cleanup can never touch the new files


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
