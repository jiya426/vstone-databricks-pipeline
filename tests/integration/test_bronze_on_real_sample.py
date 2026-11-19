"""Component-integration tests on a small slice of the REAL dataset (tests/fixtures).

They chain the same framework functions the Databricks notebooks use:
raw CSV sample -> chunking -> write as CSV / JSON -> read back with the Bronze schema -> audit columns,
and assert nothing is lost or changed (reconciliation), plus the known data defects are present.
"""
import json
import os

import pytest
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from vstone_framework.audit import AUDIT_COLUMNS, with_audit_columns
from vstone_framework.bronze import (contract_columns, first_csv_header, header_ordered_columns, string_schema,
                                     time_series_sources)
from vstone_framework.chunking import (LEVEL_COL, combine_levels, combined_columns, parse_cutoffs, reconcile_counts,
                                       with_chunk_no)
from vstone_framework.metadata import column_comment, load_data_dictionary
from vstone_framework.quality import failed, run_sanity_checks
from vstone_framework.schema_checks import compare_columns

FIX = os.path.join(os.path.dirname(__file__), "..", "fixtures")
CUTOFFS = parse_cutoffs(["2005-12-31", "2012-12-31", "2015-12-31"])
LEVELS = ["state", "metro", "county", "city", "neighborhood", "zip"]
with open(os.path.join(FIX, "fields_per_level.json")) as fh:
    FIELDS = {k.lower(): v for k, v in json.load(fh).items()}


def sample_columns(level):
    """Contract columns in the sample file's header order (same rule as the DLT pipeline)."""
    return header_ordered_columns(first_csv_header_of(level), FIELDS[level])


def first_csv_header_of(level):
    import csv
    with open(os.path.join(FIX, f"{level}_sample.csv"), newline="", encoding="utf-8") as fh:
        return next(csv.reader(fh))


def read_sample(spark, level):
    return (spark.read.option("header", True).schema(string_schema(sample_columns(level)))
            .csv(os.path.join(FIX, f"{level}_sample.csv")))


@pytest.mark.parametrize("level", LEVELS)
def test_sample_header_matches_contract(spark, level):
    header = spark.read.option("header", True).csv(os.path.join(FIX, f"{level}_sample.csv")).columns
    assert compare_columns(FIELDS[level], header) == {"missing": [], "unexpected": []}


@pytest.mark.parametrize("level", LEVELS)
def test_chunking_reconciles_on_real_rows(spark, level):
    df = read_sample(spark, level).transform(with_chunk_no(CUTOFFS))
    counts = {r.chunk_no: r["count"] for r in df.groupBy("chunk_no").count().collect()}
    assert None not in counts                                  # every real date parses
    reconcile_counts(df.count(), counts)
    bounds = {r.chunk_no: (r.lo, r.hi) for r in df.groupBy("chunk_no").agg(F.min("Date").alias("lo"),
                                                                              F.max("Date").alias("hi")).collect()}
    assert bounds[1][1] <= "2005-12-31" < bounds[2][0] <= bounds[2][1] <= "2012-12-31" < bounds[3][0]
    assert bounds[3][1] <= "2015-12-31" < bounds[4][0]


def test_json_roundtrip_restores_null_columns_with_explicit_schema(spark, tmp_path):
    """The JSON chunk omits null fields; reading with the Bronze string schema must give back identical rows."""
    original = read_sample(spark, "county").transform(with_chunk_no(CUTOFFS)).filter("chunk_no = 3").drop("chunk_no")
    original.write.mode("overwrite").json(str(tmp_path / "json"))
    raw_text = spark.read.text(str(tmp_path / "json")).first().value
    assert "null" not in raw_text                              # proves nulls were dropped by the writer
    back = spark.read.schema(string_schema(FIELDS["county"])).json(str(tmp_path / "json"))
    assertDataFrameEqual(back.select(*original.columns), original)     # JSON matches fields by NAME


def test_csv_roundtrip_keeps_corrupted_neighborhood_names_intact(spark, tmp_path):
    """Corrupted RegionNames contain a comma; writing and reading the CSV chunk must not split them."""
    original = read_sample(spark, "neighborhood")
    original.write.mode("overwrite").option("header", True).csv(str(tmp_path / "csv"))
    back = (spark.read.option("header", True).schema(string_schema(first_csv_header(str(tmp_path / "csv"))))
            .csv(str(tmp_path / "csv")))
    assertDataFrameEqual(back, original)
    assert back.filter(~F.col("RegionName").rlike("^[0-9]+$")).count() == 302


def test_known_defects_present_in_sample(spark):
    county = read_sample(spark, "county").select("RegionName").distinct().collect()
    assert {"1003", "10001"} == {r.RegionName for r in county}          # FIPS without leading zero
    zips = {r.RegionName for r in read_sample(spark, "zip").select("RegionName").distinct().collect()}
    assert "01001" in zips                                               # leading zero kept as STRING
    states = {r.RegionName for r in read_sample(spark, "state").select("RegionName").distinct().collect()}
    assert "UnitedStates" in states                                      # national row mixed in


def test_bronze_load_of_sample_passes_sanity_checks(spark):
    """Simulates the 4 Bronze loads of the combined sample (one table per chunk file) and runs the production checks."""
    combined = combine_levels({lvl: read_sample(spark, lvl) for lvl in LEVELS}, combined_columns(FIELDS))
    chunked = combined.transform(with_chunk_no(CUTOFFS))
    for src in time_series_sources():
        cols = contract_columns(src, FIELDS)
        chunk = chunked.filter(f"chunk_no = {src.chunk_no}").drop("chunk_no")
        expected_rows = chunk.count()
        loaded = chunk.transform(with_audit_columns(src.source, "test-run", F.lit(src.file_name)))
        if src.has_rescued:
            loaded = loaded.withColumn("_rescued_data", F.lit(None).cast("string"))
        results = run_sanity_checks(loaded, src.table, src.key_columns, expected_rows, cols, AUDIT_COLUMNS,
                                    rescued_column="_rescued_data" if src.has_rescued else None,
                                    allowed_values={LEVEL_COL: LEVELS})
        assert failed(results) == [], [r for r in failed(results)]


def test_every_sample_column_gets_a_real_dictionary_comment():
    d = load_data_dictionary(os.path.join(FIX, "DataDictionary.csv"))
    for level in LEVELS:
        for col in FIELDS[level]:
            assert not column_comment(col, d).startswith("Zillow metric"), col


def test_chunking_is_ansi_safe(spark):
    """Serverless runs with ANSI mode: a malformed date must give NULL chunk, not crash the job."""
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        df = spark.createDataFrame([("2010-01-31",), ("not-a-date",)], "Date string").transform(with_chunk_no(CUTOFFS))
        assert sorted([r.chunk_no for r in df.collect()], key=lambda x: (x is None, x)) == [2, None]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", "false")


def test_csv_schema_in_wrong_order_shifts_values_positional_trap(spark):
    """Regression test for a real bug found on the sample: Spark applies a CSV schema by POSITION.
    fields_per_level.json order differs from the file header, so using it directly corrupts columns."""
    contract_order = FIELDS["zip"]
    header_order = first_csv_header_of("zip")
    assert contract_order != header_order and sorted(contract_order) == sorted(header_order)
    wrong = (spark.read.option("header", True).schema(string_schema(contract_order))
             .csv(os.path.join(FIX, "zip_sample.csv")))
    right = read_sample(spark, "zip")
    assert right.filter("RegionName = '01001'").count() == 261
    assert wrong.filter("RegionName = '01001'").count() == 0          # values landed in other columns
    with pytest.raises(ValueError, match="does not match contract"):
        header_ordered_columns(header_order[:-1] + ["Extra"], contract_order)
