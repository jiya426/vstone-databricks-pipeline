"""Unit tests for each Silver transformation (used via DataFrame.transform)."""
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from vstone_framework.silver import (REASON_COL, cast_types, clean_reference, dedupe_latest, drop_empty_rows,
                                     flag_quality_issues, quarantine_frame, reconcile_silver, split_quarantine,
                                     union_bronze_sources)

METRICS = ["ZHVI_AllHomes"]
SCHEMA = "Date string, RegionName string, ZHVI_AllHomes string, _rescued_data string"


def test_union_adds_missing_rescued_column_and_keeps_lineage(spark):
    csv = spark.createDataFrame([("2000-01-31", "01001", "1", "copy_into_csv", "f1", None)],
                                "Date string, RegionName string, ZHVI_AllHomes string, source string, source_file string, load_dt timestamp")
    js = spark.createDataFrame([("2014-01-31", "01001", "2", None, "autoloader_json", "f2", None)],
                               "Date string, RegionName string, ZHVI_AllHomes string, _rescued_data string, source string, source_file string, load_dt timestamp")
    out = union_bronze_sources([csv, js], ["Date", "RegionName", "ZHVI_AllHomes"], "_rescued_data")
    assert out.count() == 2
    assert out.columns == ["Date", "RegionName", "ZHVI_AllHomes", "_rescued_data", "source", "source_file", "bronze_load_dt"]
    assert out.filter(F.col("_rescued_data").isNotNull()).count() == 0


def test_each_quality_rule(spark):
    rows = [("2010-01-31", "10001", "1.5", None),      # valid
            (None, "10001", "1", None),                # NULL_KEY
            ("2010-13-45", "10001", "1", None),        # BAD_DATE
            ("2010-01-31", "UniqueNameX,dtype:object", "1", None),   # INVALID_REGION
            ("2010-01-31", "10002", "1", '{"x":1}'),   # RESCUED_DATA
            ("2010-01-31", "10003", "abc", None)]      # NON_NUMERIC
    df = spark.createDataFrame(rows, SCHEMA).transform(flag_quality_issues(METRICS, "^[0-9]+$", "_rescued_data"))
    assert [r[REASON_COL] for r in df.collect()] == [None, "NULL_KEY", "BAD_DATE", "INVALID_REGION", "RESCUED_DATA", "NON_NUMERIC"]
    valid, bad = split_quarantine(df)
    assert valid.count() == 1 and bad.count() == 5 and REASON_COL not in valid.columns


def test_cast_types_and_drop_empty(spark):
    df = spark.createDataFrame([("2010-01-31", "a", "1.5", "2"), ("2010-02-28", "b", None, None)],
                               "Date string, RegionName string, ZHVI_AllHomes string, ZRI_AllHomes string")
    typed = df.transform(cast_types(["ZHVI_AllHomes", "ZRI_AllHomes"]))
    assert dict(typed.dtypes) == {"Date": "date", "RegionName": "string", "ZHVI_AllHomes": "double", "ZRI_AllHomes": "double"}
    kept = typed.transform(drop_empty_rows(["ZHVI_AllHomes", "ZRI_AllHomes"]))
    assert [r.RegionName for r in kept.collect()] == ["a"]


def test_dedupe_keeps_latest_load(spark):
    df = spark.createDataFrame(
        [("2010-01-31", "a", 1.0, "f_old", "2026-01-01 00:00:00"), ("2010-01-31", "a", 2.0, "f_new", "2026-02-01 00:00:00"),
         ("2010-01-31", "b", 3.0, "f", "2026-01-01 00:00:00")],
        "Date string, RegionName string, v double, source_file string, bronze_load_dt string"
    ).withColumn("bronze_load_dt", F.to_timestamp("bronze_load_dt"))
    out = df.transform(dedupe_latest(["Date", "RegionName"])).select("RegionName", "v")
    assertDataFrameEqual(out, spark.createDataFrame([("a", 2.0), ("b", 3.0)], "RegionName string, v double"))


def test_quarantine_frame_keeps_raw_row_and_explains_reason(spark):
    bad = spark.createDataFrame([("2010-01-31", "Bad,Name", "1", None, "INVALID_REGION", "dlt_csv", "f")],
                                SCHEMA + ", _reason string, source string, source_file string")
    q = quarantine_frame(bad, "neighborhood", ["Date", "RegionName", "ZHVI_AllHomes"], "_rescued_data", "run1").first()
    assert q.reason_code == "INVALID_REGION" and q.reason_detail.startswith("RegionName does not match")
    assert '"RegionName":"Bad,Name"' in q.raw_record and q.level == "neighborhood" and q.batch_id == "run1"


def test_reconcile_silver():
    assert reconcile_silver(100, 2, 3, 1, 94) is None
    assert "Bronze 100" in reconcile_silver(100, 2, 3, 1, 90)


def test_clean_reference_pads_trims_and_dedupes(spark):
    mapping = {"FIPS": "fips", "StateFIPS": "state_fips", "CountyFIPS": "county_fips", "CountyName": "county_name"}
    rules = {"key": ["fips"], "pad": {"fips": 5, "state_fips": 2, "county_fips": 3}, "upper": []}
    df = spark.createDataFrame(
        [("1003", "1", "3", " Baldwin ", "copy_into_csv", "f", "2026-01-01 00:00:00"),
         ("01003", "01", "003", "Baldwin", "copy_into_csv", "f", "2026-02-01 00:00:00"),
         (None, "1", "1", "X", "copy_into_csv", "f", "2026-01-01 00:00:00")],
        "FIPS string, StateFIPS string, CountyFIPS string, CountyName string, source string, source_file string, load_dt string"
    ).withColumn("load_dt", F.to_timestamp("load_dt"))
    out = df.transform(clean_reference(mapping, rules, "r1"))
    rows = out.collect()
    assert len(rows) == 1                                           # padded duplicates collapse, NULL key removed
    assert (rows[0].fips, rows[0].state_fips, rows[0].county_fips, rows[0].county_name) == ("01003", "01", "003", "Baldwin")
    assert out.columns[-4:] == ["load_dt", "source", "source_file", "batch_id"]
