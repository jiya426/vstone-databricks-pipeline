"""Pure, testable logic for the Data Chunking step.

The six Zillow time-series files are COMBINED into one dataset (a `RegionLevel` column says which
file each row came from) and that dataset is split by date into exactly FOUR files:

    chunk 1  Date <= cutoff_1                -> 1 CSV file   (Bronze: COPY INTO)
    chunk 2  cutoff_1 < Date <= cutoff_2     -> 1 CSV file   (Bronze: Delta Live Tables)
    chunk 3  cutoff_2 < Date <= cutoff_3     -> 1 JSON file  (Bronze: Auto Loader)
    chunk 4  Date > cutoff_3                 -> 1 XML file   (Bronze: PySpark read/write)

Folder names, file names, formats and the level column name all come from conf/vstone_config.json.
Nothing here touches storage, so every function can be unit tested locally.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from functools import reduce
from typing import Callable, Dict, List, Mapping, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .config import load_config

CHUNK_COL = "chunk_no"


@dataclass(frozen=True)
class ChunkTarget:
    """Where and how one chunk is written."""
    chunk_no: int
    folder: str        # folder inside the chunks Volume (holds exactly one file)
    file_name: str     # the single landed file
    file_format: str   # csv | json | xml
    ingestion: str     # which Bronze method reads it


_CFG = load_config()

CHUNK_TARGETS: Dict[int, ChunkTarget] = {
    int(n): ChunkTarget(int(n), c["folder"], c["file_name"], c["format"], c["method"])
    for n, c in sorted(_CFG["chunks"].items(), key=lambda kv: int(kv[0]))
}

# Source file -> short level name (the value written to the level column)
TIME_SERIES_FILES: Dict[str, str] = dict(_CFG["time_series_files"])
LEVELS: List[str] = list(_CFG["levels"])
LEVEL_COL: str = _CFG["combined"]["level_column"]
COMBINED_ENTITY: str = _CFG["combined"]["entity"]

# Reference files are not chunked; Bronze loads them straight from the raw Volume
REFERENCE_FILES: Dict[str, str] = dict(_CFG["reference_files"])

# Metadata files are never ingested (PDF rule); used for documentation/validation
METADATA_FILES = tuple(_CFG["metadata_files"])


def parse_cutoffs(values: Sequence[str]) -> List[date]:
    """Parse three 'YYYY-MM-DD' cut-off strings and check they are strictly ascending."""
    if len(values) != 3:
        raise ValueError(f"Exactly 3 cut-off dates are required, got {len(values)}")
    parsed: List[date] = []
    for v in values:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(v).strip()):
            raise ValueError(f"Cut-off '{v}' is not in YYYY-MM-DD format")
        parsed.append(date.fromisoformat(str(v).strip()))
    if not (parsed[0] < parsed[1] < parsed[2]):
        raise ValueError(f"Cut-offs must be strictly ascending, got {values}")
    return parsed


def chunk_expression(date_col: Column, cutoffs: Sequence[date]) -> Column:
    """Column expression returning 1-4 for a valid date, NULL for an unparseable one."""
    c1, c2, c3 = (F.lit(c.isoformat()).cast("date") for c in cutoffs)
    return (
        F.when(date_col.isNull(), F.lit(None).cast("int"))
        .when(date_col <= c1, F.lit(1))
        .when(date_col <= c2, F.lit(2))
        .when(date_col <= c3, F.lit(3))
        .otherwise(F.lit(4))
    )


def with_chunk_no(cutoffs: Sequence[date], date_col: str = "Date") -> Callable[[DataFrame], DataFrame]:
    """Transformation for DataFrame.transform(): adds the integer column `chunk_no`."""
    def _transform(df: DataFrame) -> DataFrame:
        # try_to_timestamp returns NULL for malformed text even with ANSI mode on (serverless default)
        parsed = F.try_to_timestamp(F.col(date_col), F.lit("yyyy-MM-dd")).cast("date")
        return df.withColumn(CHUNK_COL, chunk_expression(parsed, cutoffs))
    return _transform


# ------------------------------------------------------------------ combining the 6 levels
def combined_columns(level_fields: Mapping[str, Sequence[str]], levels: Sequence[str] = None,
                     level_col: str = LEVEL_COL) -> List[str]:
    """Column contract of the combined dataset: Date, RegionName, level column, then every metric of
    every level once, in first-seen order (levels in config order). Deterministic, so all 4 files
    and all 4 Bronze tables share exactly the same columns."""
    levels = list(levels or LEVELS)
    keys = ["Date", "RegionName"]
    metrics: List[str] = []
    for lvl in levels:
        for c in level_fields[lvl]:
            if c not in keys and c not in metrics:
                metrics.append(c)
    return keys + [level_col] + metrics


def tag_level(level: str, columns: Sequence[str], level_col: str = LEVEL_COL) -> Callable[[DataFrame], DataFrame]:
    """Transformation: add the level column and conform to the combined column list
    (metrics this level does not report become NULL strings)."""
    def _transform(df: DataFrame) -> DataFrame:
        present = set(df.columns)
        cols = []
        for c in columns:
            if c == level_col:
                cols.append(F.lit(level).alias(level_col))
            elif c in present:
                cols.append(F.col(f"`{c}`").cast("string").alias(c))
            else:
                cols.append(F.lit(None).cast("string").alias(c))
        return df.select(*cols)
    return _transform


def combine_levels(frames: Mapping[str, DataFrame], columns: Sequence[str],
                   level_col: str = LEVEL_COL) -> DataFrame:
    """Stack the per-level DataFrames into one combined dataset with identical columns."""
    parts = [df.transform(tag_level(level, columns, level_col)) for level, df in frames.items()]
    return reduce(lambda a, b: a.unionByName(b), parts)


# ------------------------------------------------------------------ paths and bookkeeping
def chunk_folder(chunks_root: str, chunk_no: int) -> str:
    """e.g. /Volumes/vstone/landing/chunks/chunk3_json"""
    if chunk_no not in CHUNK_TARGETS:
        raise ValueError(f"Unknown chunk number {chunk_no}")
    return f"{chunks_root.rstrip('/')}/{CHUNK_TARGETS[chunk_no].folder}"


def chunk_file(chunks_root: str, chunk_no: int) -> str:
    """e.g. /Volumes/vstone/landing/chunks/chunk3_json/zillow_time_series_chunk3.json"""
    return f"{chunk_folder(chunks_root, chunk_no)}/{CHUNK_TARGETS[chunk_no].file_name}"


def staging_folder(chunks_root: str, chunk_no: int) -> str:
    """Temporary Spark output folder; the single part file is moved out of it, then it is deleted."""
    return f"{chunks_root.rstrip('/')}/{_CFG['combined']['staging_folder']}/{CHUNK_TARGETS[chunk_no].folder}"


def reconcile_counts(source_rows: int, chunk_rows: Dict[int, int]) -> None:
    """Fail loudly if rows were lost or duplicated while splitting."""
    total = sum(chunk_rows.values())
    if total != source_rows:
        raise AssertionError(
            f"Row count mismatch: source={source_rows}, sum of chunks={total}, per chunk={chunk_rows}"
        )


def reconcile_levels(expected: Mapping[str, int], level_counts: Mapping[str, Mapping[str, int]]) -> List[str]:
    """Per level, rows over all chunks must equal the source file's rows. Returns error messages.

    `level_counts` = {chunk_no (str): {level: rows}} as stored in the chunk manifest.
    """
    totals: Dict[str, int] = {}
    for per_level in level_counts.values():
        for lvl, n in per_level.items():
            totals[lvl] = totals.get(lvl, 0) + int(n)
    errors = [f"{lvl}: chunks hold {totals.get(lvl, 0):,} rows, source has {n:,}"
              for lvl, n in expected.items() if totals.get(lvl, 0) != n]
    unknown = sorted(set(totals) - set(expected))
    if unknown:
        errors.append(f"Unknown levels in chunks: {unknown}")
    return errors


def invalid_xml_names(columns: Sequence[str]) -> List[str]:
    """Column names that cannot be XML element names (spaces, leading digit, symbols)."""
    pattern = re.compile(r"^[A-Za-z_][A-Za-z0-9_.\-]*$")
    return [c for c in columns if not pattern.match(c) or c.lower().startswith("xml")]


def plan_action(manifest_status: str | None, output_exists: bool, force: bool) -> str:
    """Decide what a run should do for one chunk - this is what makes re-runs safe.

    Returns one of:
      'write'   - first run, or previous attempt failed / output missing
      'skip'    - already written and verified; re-writing would give COPY INTO / Auto Loader a
                  changed file and load the rows twice
      'rewrite' - force=True: overwrite on purpose (downstream must be reset)
    """
    if force:
        return "rewrite"
    if manifest_status == "SUCCESS" and output_exists:
        return "skip"
    return "write"


# ------------------------------------------------------------------ migration from the first layout
def legacy_layout(levels: Sequence[str] = None) -> Dict[str, List[str]]:
    """Objects of the first design (every file split into 4 chunks = 24 folders and 24 Bronze tables),
    listed in config -> legacy_layout. Removed by the first task of the Data Chunking job (no-op once gone)."""
    legacy = _CFG["legacy_layout"]
    levels = list(levels or LEVELS)
    return {
        "chunk_folders": list(legacy["chunk_folders"]),
        "bronze_tables": [f"{lvl}_{suffix}" for suffix in legacy["bronze_table_suffixes"] for lvl in levels],
        "checkpoint_folders": list(legacy["checkpoint_folders"]),
    }
