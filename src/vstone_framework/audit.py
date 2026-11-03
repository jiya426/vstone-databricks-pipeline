"""Audit columns added to every layer (PDF: "Audit columns mandatory in all layers").

    load_dt      TIMESTAMP  when the row was loaded into this layer
    source       STRING     ingestion method + format, e.g. 'copy_into_csv'
    source_file  STRING     full path of the file the row came from
    batch_id     STRING     job run id (or 'manual' for interactive runs)
"""
from __future__ import annotations

from typing import Callable, List, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

AUDIT_COLUMNS: List[str] = ["load_dt", "source", "source_file", "batch_id"]

AUDIT_COLUMN_COMMENTS = {
    "load_dt": "Audit: timestamp (UTC) when the row was loaded into this table",
    "source": "Audit: ingestion method and source format, e.g. copy_into_csv",
    "source_file": "Audit: full path of the source file the row was read from",
    "batch_id": "Audit: Databricks job run id that loaded the row ('manual' when run interactively)",
}

# Allowed values for `source` - one per ingestion path required by the PDF
SOURCES = {
    "copy_into_csv": "COPY INTO, chunk 1 CSV",
    "dlt_csv": "Delta Live Tables, chunk 2 CSV",
    "autoloader_json": "Auto Loader, chunk 3 JSON",
    "pyspark_xml": "PySpark read/write, chunk 4 XML",
}


def with_audit_columns(source: str, batch_id: str,
                       source_file: Optional[Column] = None) -> Callable[[DataFrame], DataFrame]:
    """Transformation for DataFrame.transform(): appends the four audit columns.

    `source_file` defaults to the file metadata column `_metadata.file_path`, which exists
    when reading files on Databricks; tests pass a literal instead.
    """
    if source not in SOURCES:
        raise ValueError(f"Unknown source '{source}', expected one of {sorted(SOURCES)}")
    file_col = source_file if source_file is not None else F.col("_metadata.file_path")

    def _transform(df: DataFrame) -> DataFrame:
        return (df.withColumn("load_dt", F.current_timestamp())
                  .withColumn("source", F.lit(source))
                  .withColumn("source_file", file_col.cast("string"))
                  .withColumn("batch_id", F.lit(str(batch_id))))
    return _transform
