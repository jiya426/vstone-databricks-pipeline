"""ops.chunk_manifest: one row per landed chunk file - the memory that makes chunking re-runs safe."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, Mapping, Optional

MANIFEST_DDL = """CREATE TABLE IF NOT EXISTS {table} (
  chunk_no     INT     COMMENT '1-4; 0 marks a run-level failure',
  file_name    STRING  COMMENT 'Landed chunk file name',
  entity       STRING  COMMENT 'Bronze entity the file feeds (time_series)',
  file_format  STRING  COMMENT 'csv, json or xml',
  output_path  STRING  COMMENT 'Full path of the landed file',
  row_count    BIGINT  COMMENT 'Rows written, verified by reading the file back',
  level_counts STRING  COMMENT 'JSON: rows per RegionLevel inside this file (reconciliation per source file)',
  min_date     STRING, max_date STRING,
  cutoffs      STRING  COMMENT 'Cut-off dates used, to detect configuration changes',
  status       STRING  COMMENT 'SUCCESS or FAILED',
  error        STRING,
  run_id       STRING,
  updated_at   TIMESTAMP
) COMMENT 'One row per landed chunk file (4 files): status of the Data Chunking job'"""


def ensure_manifest(spark, table: str) -> None:
    spark.sql(MANIFEST_DDL.format(table=table))


def manifest_lookup(spark, table: str) -> Dict[int, object]:
    return {r.chunk_no: r for r in spark.table(table).collect()}


def level_counts_json(counts: Mapping[str, int]) -> str:
    return json.dumps({k: int(v) for k, v in sorted(counts.items())})


def record(spark, table: str, chunk_no: int, file_name: Optional[str], entity: str, fmt: Optional[str],
           path: Optional[str], rows: Optional[int], level_counts: Optional[str], min_d: Optional[str],
           max_d: Optional[str], cutoffs: str, status: str, run_id: str, error: Optional[str] = None) -> None:
    """Upsert one manifest row (MERGE on chunk_no)."""
    row = [(chunk_no, file_name, entity, fmt, path, rows, level_counts, min_d, max_d, cutoffs, status,
            (error or "")[:2000] or None, run_id, datetime.now(timezone.utc))]
    spark.createDataFrame(row, spark.table(table).schema).createOrReplaceTempView("_manifest_upd")
    spark.sql(f"""MERGE INTO {table} t USING _manifest_upd s
                  ON t.chunk_no = s.chunk_no
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def clear_run_failure(spark, table: str) -> None:
    """A successful run removes the run-level FAILED marker (chunk_no = 0) of an earlier attempt."""
    spark.sql(f"DELETE FROM {table} WHERE chunk_no = 0")
