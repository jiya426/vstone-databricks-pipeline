"""Unit tests for post-ingestion sanity checks."""
from vstone_framework.quality import failed, run_sanity_checks

COLS, AUDIT = ["Date", "RegionName"], ["load_dt", "source", "source_file", "batch_id"]
SCHEMA = "Date string, RegionName string, load_dt string, source string, source_file string, batch_id string"


def checks(spark, rows, expected_rows=None):
    df = spark.createDataFrame(rows, SCHEMA)
    return {r.check_name: r.status for r in run_sanity_checks(df, "t", COLS, expected_rows, COLS, AUDIT)}


def test_clean_data_passes_all_checks(spark):
    rows = [("2010-01-31", "a", "ts", "s", "f", "1"), ("2010-01-31", "b", "ts", "s", "f", "1")]
    result = checks(spark, rows, expected_rows=2)
    assert result == {"row_count_matches_source": "PASS", "no_null_keys": "PASS", "no_duplicate_keys": "PASS",
                      "audit_columns_populated": "PASS", "columns_match_contract": "PASS"}


def test_each_problem_is_detected(spark):
    rows = [("2010-01-31", "a", "ts", "s", "f", "1"),
            ("2010-01-31", "a", "ts", "s", "f", "1"),      # duplicate key (file loaded twice)
            (None, "b", "ts", "s", "f", "1"),              # null key
            ("2010-02-28", "c", "ts", "s", None, "1")]     # missing audit value
    result = checks(spark, rows, expected_rows=3)
    assert result["row_count_matches_source"] == "FAIL"
    assert result["no_null_keys"] == "FAIL"
    assert result["no_duplicate_keys"] == "FAIL"
    assert result["audit_columns_populated"] == "FAIL"


def test_extra_column_fails_contract(spark):
    df = spark.createDataFrame([("2010-01-31", "a", "x", "ts", "s", "f", "1")],
                               "Date string, RegionName string, Extra string, " + SCHEMA.split(", ", 2)[2])
    res = run_sanity_checks(df, "t", COLS, None, COLS, AUDIT)
    assert [r.check_name for r in failed(res)] == ["columns_match_contract"]
