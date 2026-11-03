"""Run one action per table with uniform logging and error handling.

Every ingestion notebook used to repeat: try -> load -> log SUCCESS -> except -> log FAILED ->
continue with the other tables -> fail the task at the end. That loop now lives here.
"""
from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterable, List, Optional, Tuple, TypeVar

T = TypeVar("T")


@dataclass
class TableResult:
    table: str
    status: str                  # SUCCESS | FAILED
    rows: Optional[int] = None
    files: Optional[int] = None
    error: Optional[str] = None


# logger(table, rows, files, status, started_at, error)
Logger = Callable[[str, Optional[int], Optional[int], str, datetime, Optional[str]], None]


def run_per_table(items: Iterable[T], table_of: Callable[[T], str],
                  action: Callable[[T], Tuple[int, Optional[int]]], logger: Logger) -> List[TableResult]:
    """`action(item)` returns (rows, files). Failures are logged and collected, never stop the loop."""
    results: List[TableResult] = []
    for item in items:
        table = table_of(item)
        started = datetime.now(timezone.utc)
        try:
            rows, files = action(item)
            logger(table, rows, files, "SUCCESS", started, None)
            results.append(TableResult(table, "SUCCESS", rows, files))
            print(f"OK   {table}: {rows:,} new rows" + (f" from {files} file(s)" if files is not None else ""))
        except Exception as e:  # noqa: BLE001 - every failure is recorded, then re-raised by raise_if_failed
            logger(table, None, None, "FAILED", started, str(e))
            results.append(TableResult(table, "FAILED", error=str(e)[:500]))
            print(f"FAIL {table}: {e}\n{traceback.format_exc()}")
    return results


def run_log_logger(spark, ops_schema: str, run_id: str, layer: str, step: str) -> Logger:
    """Logger that writes to ops.pipeline_run_log."""
    from .ops import log_run

    def _log(table, rows, files, status, started, error):
        log_run(spark, ops_schema, run_id, layer, step, table, rows, files, status, started, error)
    return _log


def results_rows(results: List[TableResult]) -> List[tuple]:
    return [(r.table, r.files, r.rows, r.status, r.error) for r in results]


RESULTS_SCHEMA = "table STRING, files INT, new_rows LONG, status STRING, error STRING"


def raise_if_failed(results: List[TableResult], what: str, log_table: str) -> None:
    failed = [r.table for r in results if r.status != "SUCCESS"]
    if failed:
        raise RuntimeError(f"{what} failed for {failed}; details in {log_table}")
