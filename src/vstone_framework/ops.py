"""Operational tables: run log and sanity-check results (PDF: "Add unit tests, monitoring")."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Sequence

from .config import load_config
from .quality import CheckResult


def ops_table(ops_schema: str, key: str) -> str:
    """Fully qualified ops table; names come from conf/vstone_config.json -> ops_tables."""
    return f"{ops_schema}.{load_config()['ops_tables'][key]}"


def ensure_ops_tables(spark, ops_schema: str) -> None:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ops_table(ops_schema, 'run_log')} (
        run_id STRING COMMENT 'Databricks job run id or manual',
        layer STRING COMMENT 'landing, bronze, silver or gold',
        step STRING COMMENT 'Ingestion step, e.g. copy_into_csv',
        table_name STRING COMMENT 'Target table',
        rows_inserted BIGINT COMMENT 'Rows written by this step',
        files_loaded BIGINT COMMENT 'Files processed, when known',
        status STRING COMMENT 'SUCCESS or FAILED',
        error STRING COMMENT 'Error message when FAILED',
        started_at TIMESTAMP, ended_at TIMESTAMP)
      COMMENT 'One row per table per pipeline step: monitoring and audit trail'""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ops_table(ops_schema, 'sanity_checks')} (
        run_id STRING, layer STRING, table_name STRING, check_name STRING,
        expected STRING, actual STRING, status STRING COMMENT 'PASS or FAIL', checked_at TIMESTAMP)
      COMMENT 'Result of every sanity check after each ingestion'""")


def log_run(spark, ops_schema: str, run_id: str, layer: str, step: str, table_name: str,
            rows_inserted: Optional[int], files_loaded: Optional[int], status: str,
            started_at: datetime, error: Optional[str] = None) -> None:
    row = [(run_id, layer, step, table_name, rows_inserted, files_loaded, status,
            (error or "")[:2000] or None, started_at, datetime.now(timezone.utc))]
    table = ops_table(ops_schema, "run_log")
    spark.createDataFrame(row, spark.table(table).schema).write.mode("append").saveAsTable(table)


def write_check_results(spark, ops_schema: str, run_id: str, layer: str,
                        results: Sequence[CheckResult]) -> None:
    now = datetime.now(timezone.utc)
    rows = [(run_id, layer, r.table_name, r.check_name, r.expected, r.actual, r.status, now) for r in results]
    table = ops_table(ops_schema, "sanity_checks")
    spark.createDataFrame(rows, spark.table(table).schema).write.mode("append").saveAsTable(table)
