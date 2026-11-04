"""Bronze -> Silver on the real-data fixtures, through the same functions the notebook chains."""
import csv
import json
import os

from pyspark.sql import functions as F

from vstone_framework.audit import with_audit_columns
from vstone_framework.bronze import first_csv_header, header_ordered_columns, string_schema
from vstone_framework.config import load_config
from vstone_framework.headers import header_mapping, to_snake_case
from vstone_framework.silver import (cast_types, dedupe_latest, drop_empty_rows, finalize_silver, flag_quality_issues,
                                     reconcile_silver, split_quarantine, union_bronze_sources)

FIX = os.path.join(os.path.dirname(__file__), "..", "fixtures")
CFG = load_config()
with open(os.path.join(FIX, "fields_per_level.json")) as fh:
    FIELDS = {k.lower(): v for k, v in json.load(fh).items()}


def bronze_like(spark, level, source):
    path = os.path.join(FIX, f"{level}_sample.csv")
    cols = header_ordered_columns(first_csv_header(os.path.dirname(path)) if False else next(csv.reader(open(path))),
                                  FIELDS[level])
    return (spark.read.option("header", True).schema(string_schema(cols)).csv(path)
            .transform(with_audit_columns(source, "t", F.lit(path))))


def run_silver(spark, level, duplicate_load=False):
    metrics = [c for c in FIELDS[level] if c not in ("Date", "RegionName")]
    contract = ["Date", "RegionName"] + metrics
    parts = [bronze_like(spark, level, "copy_into_csv")]
    if duplicate_load:                                   # same rows loaded twice by another method
        parts.append(bronze_like(spark, level, "dlt_csv"))
    bronze_rows = sum(p.count() for p in parts)
    union = union_bronze_sources(parts, contract, "_rescued_data")
    valid, bad = split_quarantine(union.transform(
        flag_quality_issues(metrics, CFG["silver"]["region_patterns"][level], "_rescued_data")))
    deduped = valid.transform(dedupe_latest(["Date", "RegionName"]))           # dedupe first (see silver.py)
    mapping = header_mapping(spark, contract)
    silver = (deduped.transform(cast_types(metrics)).transform(drop_empty_rows(metrics))
                     .transform(finalize_silver(mapping, contract, "t")))
    q, dd, s = bad.count(), deduped.count(), silver.count()
    return silver, bad, reconcile_silver(bronze_rows, q, dd - s, bronze_rows - q - dd, s), (bronze_rows, q, dd, s)


def test_neighborhood_corrupted_rows_are_quarantined_and_rest_balances(spark):
    silver, bad, error, (b, q, dd, s) = run_silver(spark, "neighborhood")
    assert q == 302 and set(r._reason for r in bad.select("_reason").distinct().collect()) == {"INVALID_REGION"}
    assert error is None and s == b - 302
    assert silver.filter(~F.col("region_name").rlike("^[0-9]+$")).count() == 0


def test_county_silver_is_typed_snake_case_and_unique(spark):
    silver, bad, error, _ = run_silver(spark, "county")
    assert error is None and bad.count() == 0
    types = dict(silver.dtypes)
    assert types["date"] == "date" and types["region_name"] == "string" and types["zhvi_all_homes"] == "double"
    assert set(silver.columns) == {to_snake_case(c) for c in FIELDS["county"]} | {"load_dt", "source", "source_file", "batch_id"}
    assert silver.groupBy("date", "region_name").count().filter("count > 1").count() == 0


def test_duplicate_load_is_removed_and_reconciled(spark):
    silver, _, error, (b, q, dd, s) = run_silver(spark, "zip", duplicate_load=True)
    assert error is None and b - q - dd == b // 2 and s == b // 2       # every row was loaded twice


def test_full_silver_materialises_quickly(spark):
    """Regression: materialising every column must not blow up the optimiser (dedupe-after-cast did)."""
    silver, _, _, (_, _, _, s) = run_silver(spark, "metro")
    assert len(silver.collect()) == s


def test_silver_values_match_source(spark):
    silver, _, _, _ = run_silver(spark, "zip")
    v = silver.filter("region_name = '01001' AND date = DATE'2000-01-31'").select("zhvi_all_homes").first()[0]
    assert v == 113500.0                                                  # same value as Bronze check query B
