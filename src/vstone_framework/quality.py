"""Sanity checks run after every ingestion (PDF: "Sanity check for pipelines for each ingestion")."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CheckResult:
    table_name: str
    check_name: str
    expected: str
    actual: str
    status: str          # PASS | FAIL


def _result(table: str, name: str, expected, actual, ok: bool) -> CheckResult:
    return CheckResult(table, name, str(expected), str(actual), "PASS" if ok else "FAIL")


def run_sanity_checks(df: DataFrame, table_name: str, key_columns: Sequence[str],
                      expected_rows: Optional[int], expected_columns: Sequence[str],
                      audit_columns: Sequence[str], rescued_column: Optional[str] = None,
                      allowed_values: Optional[Dict[str, Sequence[str]]] = None) -> List[CheckResult]:
    """Row count vs source, null keys, duplicate keys, null audit values, exact column set,
    for JSON/XML/DLT tables no values captured in the rescued-data column, and optionally that a
    column only holds allowed values (e.g. RegionLevel in the 6 configured levels)."""
    results: List[CheckResult] = []
    stats = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.greatest(*[F.col(f"`{k}`").isNull().cast("int") for k in key_columns], F.lit(0))).alias("null_keys"),
        F.sum(F.greatest(*[F.col(a).isNull().cast("int") for a in audit_columns], F.lit(0))).alias("null_audit"),
    ).first()
    rows = stats["rows"]
    if expected_rows is not None:
        results.append(_result(table_name, "row_count_matches_source", expected_rows, rows, rows == expected_rows))
    results.append(_result(table_name, "no_null_keys", 0, stats["null_keys"] or 0, (stats["null_keys"] or 0) == 0))
    dups = df.groupBy(*[F.col(f"`{k}`") for k in key_columns]).count().filter("count > 1").count()
    results.append(_result(table_name, "no_duplicate_keys", 0, dups, dups == 0))
    results.append(_result(table_name, "audit_columns_populated", 0, stats["null_audit"] or 0,
                           (stats["null_audit"] or 0) == 0))
    if rescued_column:
        rescued = df.filter(F.col(rescued_column).isNotNull()).count()
        results.append(_result(table_name, "no_rescued_data", 0, rescued, rescued == 0))
    for col, allowed in (allowed_values or {}).items():
        bad = df.filter(~F.col(col).isin(list(allowed)) | F.col(col).isNull()).count()
        results.append(_result(table_name, f"{col}_in_allowed_values", 0, bad, bad == 0))
    expected_set = list(expected_columns) + ([rescued_column] if rescued_column else []) + list(audit_columns)
    actual = df.columns
    results.append(_result(table_name, "columns_match_contract", len(expected_set), len(actual),
                           sorted(actual) == sorted(expected_set)))
    return results


def failed(results: Sequence[CheckResult]) -> List[CheckResult]:
    return [r for r in results if r.status != "PASS"]
