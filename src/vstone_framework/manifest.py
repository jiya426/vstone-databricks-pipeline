"""ops.chunk_manifest: one row per (source file, chunk) - the memory that makes chunking re-runs safe."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

MANIFEST_DDL = """CREATE TABLE IF NOT EXISTS {table} (
  source_file STRING  COMMENT 'Raw file name in the raw Volume',
  level       STRING  COMMENT 'state, metro, county, city, neighborhood, zip or reference name',
  chunk_no    INT     COMMENT '1-4; 1 for reference files; 0 marks a file-level failure',
  file_format STRING  COMMENT 'csv, json or xml',
  output_path STRING  COMMENT 'Folder the chunk was written to',
  row_count   BIGINT  COMMENT 'Rows written, verified by reading the output back',
  min_date    STRING, max_date STRING,
  cutoffs     STRING  COMMENT 'Cut-off dates used, to detect configuration changes',
  status      STRING  COMMENT 'SUCCESS or FAILED',
  error       STRING,
  run_id      STRING,
  updated_at  TIMESTAMP
) COMMENT 'One row per (source file, chunk): status of the Data Chunking job'"""


def ensure_manifest(spark, table: str) -> None:
    spark.sql(MANIFEST_DDL.format(table=table))


def manifest_lookup(spark, table: str) -> Dict[Tuple[str, int], object]:
    return {(r.source_file, r.chunk_no): r for r in spark.table(table).collect()}


def record(spark, table: str, source_file: str, level: str, chunk_no: int, fmt: Optional[str],
           path: Optional[str], rows: Optional[int], min_d: Optional[str], max_d: Optional[str],
           cutoffs: str, status: str, run_id: str, error: Optional[str] = None) -> None:
    """Upsert one manifest row (MERGE on source_file + chunk_no)."""
    row = [(source_file, level, chunk_no, fmt, path, rows, min_d, max_d, cutoffs, status,
            (error or "")[:2000] or None, run_id, datetime.now(timezone.utc))]
    spark.createDataFrame(row, spark.table(table).schema).createOrReplaceTempView("_manifest_upd")
    spark.sql(f"""MERGE INTO {table} t USING _manifest_upd s
                  ON t.source_file = s.source_file AND t.chunk_no = s.chunk_no
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def clear_file_failure(spark, table: str, source_file: str) -> None:
    """A successful run removes the file-level FAILED marker (chunk_no = 0) of an earlier attempt."""
    spark.sql(f"DELETE FROM {table} WHERE source_file = '{source_file}' AND chunk_no = 0")
