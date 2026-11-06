"""End-to-end check of the trainer's layout on REAL data (tests/fixtures):

    6 raw files -> combined dataset (RegionLevel) -> 4 chunks -> exactly 4 landed files (CSV, CSV, JSON, XML*)
    -> read back with the Bronze schema -> split by level -> Silver rules -> reconciliation

*XML needs Databricks' native XML reader, so locally chunk 4 is written as JSON; the XML path is covered on Databricks
by the Data Chunking job (read-back count) and the Bronze sanity checks.
"""
import json
import os

import pytest
from pyspark.sql import functions as F

from vstone_framework.audit import with_audit_columns
from vstone_framework.bronze import first_csv_header, header_ordered_columns, string_schema, time_series_sources
from vstone_framework.chunk_io import LocalFs, write_single_file
from vstone_framework.chunking import (CHUNK_COL, CHUNK_TARGETS, LEVEL_COL, chunk_file, combine_levels,
                                       combined_columns, parse_cutoffs, reconcile_levels, staging_folder,
                                       with_chunk_no)
from vstone_framework.config import load_config
from vstone_framework.silver import (dedupe_latest, flag_quality_issues, reconcile_silver, split_quarantine,
                                     union_bronze_sources)

FIX = os.path.join(os.path.dirname(__file__), "..", "fixtures")
CFG = load_config()
CUTOFFS = parse_cutoffs(["2005-12-31", "2012-12-31", "2015-12-31"])
LEVELS = CFG["levels"]
with open(os.path.join(FIX, "fields_per_level.json")) as fh:
    FIELDS = {k.lower(): v for k, v in json.load(fh).items()}
COLUMNS = combined_columns(FIELDS)


def read_sample(spark, level):
    path = os.path.join(FIX, f"{level}_sample.csv")
    cols = header_ordered_columns(first_csv_header(path), FIELDS[level])
    return spark.read.option("header", True).schema(string_schema(cols)).csv(path)


@pytest.fixture(scope="module")
def landed(spark, tmp_path_factory):
    """Run the chunking steps exactly as 02_data_chunking does; return (chunks_root, rows per level per chunk)."""
    root = str(tmp_path_factory.mktemp("chunks"))
    combined = combine_levels({lvl: read_sample(spark, lvl) for lvl in LEVELS}, COLUMNS).transform(with_chunk_no(CUTOFFS))
    counts = {}
    for r in combined.groupBy(CHUNK_COL, LEVEL_COL).count().collect():
        counts.setdefault(r[CHUNK_COL], {})[r[LEVEL_COL]] = r["count"]
    for n, t in CHUNK_TARGETS.items():
        fmt = "json" if t.file_format == "xml" else t.file_format          # see module docstring
        write_single_file(combined.filter(F.col(CHUNK_COL) == n).drop(CHUNK_COL), fmt,
                          staging_folder(root, n), chunk_file(root, n), LocalFs())
    return root, counts


def read_chunk(spark, root, n):
    t, path = CHUNK_TARGETS[n], chunk_file(root, n)
    if t.file_format == "csv":
        return spark.read.option("header", True).schema(string_schema(header_ordered_columns(first_csv_header(path), COLUMNS))).csv(path)
    return spark.read.schema(string_schema(COLUMNS)).json(path)            # JSON matches fields by name


def test_exactly_four_files_land_one_per_folder(landed):
    root, _ = landed
    assert sorted(os.listdir(root)) == sorted(t.folder for t in CHUNK_TARGETS.values())   # staging is gone
    for n, t in CHUNK_TARGETS.items():
        assert os.listdir(os.path.join(root, t.folder)) == [t.file_name]


def test_every_source_row_is_in_exactly_one_file(spark, landed):
    root, counts = landed
    expected = {lvl: read_sample(spark, lvl).count() for lvl in LEVELS}
    assert reconcile_levels(expected, counts) == []
    back = {n: read_chunk(spark, root, n).groupBy(LEVEL_COL).count().collect() for n in CHUNK_TARGETS}
    assert {n: {r[LEVEL_COL]: r["count"] for r in rows} for n, rows in back.items()} == counts


def test_values_survive_the_round_trip(spark, landed):
    root, _ = landed
    raw = {r.Date: r.ZHVI_AllHomes for r in read_sample(spark, "zip").filter("RegionName = '01001'").collect()}
    zhvi = {}
    for n in CHUNK_TARGETS:
        for r in (read_chunk(spark, root, n).filter((F.col("RegionName") == "01001") & (F.col(LEVEL_COL) == "zip"))
                  .select("Date", "ZHVI_AllHomes").collect()):
            zhvi[r.Date] = r.ZHVI_AllHomes
    assert len(zhvi) == 261                       # all months of ZIP 01001, across the 4 files
    assert zhvi == raw                            # every value identical to the raw file (leading zero kept)


def test_silver_splits_the_four_tables_by_level_and_reconciles(spark, landed):
    root, _ = landed
    bronze = [read_chunk(spark, root, s.chunk_no).transform(with_audit_columns(s.source, "t", F.lit(s.file_name)))
              for s in time_series_sources()]
    total = 0
    for level in LEVELS:
        metrics = [c for c in FIELDS[level] if c not in ("Date", "RegionName")]
        contract = ["Date", "RegionName"] + metrics
        parts = [b.filter(F.col(LEVEL_COL) == level) for b in bronze]
        bronze_rows = sum(p.count() for p in parts)
        total += bronze_rows
        valid, bad = split_quarantine(union_bronze_sources(parts, contract, "_rescued_data").transform(
            flag_quality_issues(metrics, CFG["silver"]["region_patterns"][level], "_rescued_data")))
        q, dd = bad.count(), valid.transform(dedupe_latest(["Date", "RegionName"])).count()
        assert q == CFG["silver"]["expected_quarantine_rows"][level], level       # 302 neighborhood rows
        assert reconcile_silver(bronze_rows, q, 0, bronze_rows - q - dd, dd) is None
        assert bronze_rows - q - dd == 0, f"{level}: unexpected duplicates"
    assert total == sum(b.count() for b in bronze)                               # every row maps to a level
