"""Pure, testable logic for the Data Chunking step.

The six Zillow time-series files are split into four chunks by date:

    chunk 1  Date <= cutoff_1                -> CSV (initial load,   COPY INTO)
    chunk 2  cutoff_1 < Date <= cutoff_2     -> CSV (incremental,    DLT)
    chunk 3  cutoff_2 < Date <= cutoff_3     -> JSON                 (Auto Loader)
    chunk 4  Date > cutoff_3                 -> XML                  (PySpark read/write)

Nothing here touches storage, so every function can be unit tested locally.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Callable, Dict, List, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

CHUNK_COL = "chunk_no"


@dataclass(frozen=True)
class ChunkTarget:
    """Where and how one chunk is written."""
    chunk_no: int
    folder: str        # sub-folder inside the chunks Volume
    file_format: str   # csv | json | xml
    ingestion: str     # documentation: which Bronze method reads it


from .config import load_config

_CFG = load_config()

CHUNK_TARGETS: Dict[int, ChunkTarget] = {
    int(n): ChunkTarget(int(n), c["folder"], c["format"], c["method"]) for n, c in _CFG["chunks"].items()
}

# Source file -> short level name used in folder and table names
TIME_SERIES_FILES: Dict[str, str] = dict(_CFG["time_series_files"])

# Reference files are not chunked; they travel with chunk 1 (assumption A2)
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


def chunk_output_path(chunks_root: str, chunk_no: int, level: str) -> str:
    """e.g. /Volumes/vstone/landing/chunks/json/zip"""
    if chunk_no not in CHUNK_TARGETS:
        raise ValueError(f"Unknown chunk number {chunk_no}")
    return f"{chunks_root.rstrip('/')}/{CHUNK_TARGETS[chunk_no].folder}/{level}"


def reconcile_counts(source_rows: int, chunk_rows: Dict[int, int]) -> None:
    """Fail loudly if rows were lost or duplicated while splitting."""
    total = sum(chunk_rows.values())
    if total != source_rows:
        raise AssertionError(
            f"Row count mismatch: source={source_rows}, sum of chunks={total}, per chunk={chunk_rows}"
        )


def invalid_xml_names(columns: Sequence[str]) -> List[str]:
    """Column names that cannot be XML element names (spaces, leading digit, symbols)."""
    pattern = re.compile(r"^[A-Za-z_][A-Za-z0-9_.\-]*$")
    return [c for c in columns if not pattern.match(c) or c.lower().startswith("xml")]


def plan_action(manifest_status: str | None, output_exists: bool, force: bool) -> str:
    """Decide what a run should do for one (file, chunk) - this is what makes re-runs safe.

    Returns one of:
      'write'   - first run, or previous attempt failed / output missing
      'skip'    - already written and verified; re-writing would create new file
                  names and make COPY INTO / Auto Loader load the rows twice
      'rewrite' - force=True: overwrite on purpose (downstream must be reset)
    """
    if force:
        return "rewrite"
    if manifest_status == "SUCCESS" and output_exists:
        return "skip"
    return "write"
