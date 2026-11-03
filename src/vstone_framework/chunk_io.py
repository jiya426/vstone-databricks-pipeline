"""Reading and writing chunk files (used by the Data Chunking job)."""
from __future__ import annotations

import math

from pyspark.sql import DataFrame

from .config import load_config


def write_chunk(df: DataFrame, fmt: str, path: str, n_rows: int, rows_per_file: int) -> None:
    """Write one chunk in its target format, sized to about `rows_per_file` rows per file."""
    xml = load_config()["xml"]
    writer = df.repartition(max(1, math.ceil(n_rows / rows_per_file))).write.mode("overwrite")
    if fmt == "csv":
        writer.option("header", True).csv(path)
    elif fmt == "json":
        writer.json(path)                      # null fields are omitted by the JSON writer
    elif fmt == "xml":
        writer.format("xml").option("rootTag", xml["root_tag"]).option("rowTag", xml["row_tag"]).save(path)
    else:
        raise ValueError(f"Unsupported chunk format '{fmt}'")


def read_back_count(spark, fmt: str, path: str) -> int:
    """Count the rows actually written - proves the chunk is readable in its format."""
    if fmt == "csv":
        return spark.read.option("header", True).csv(path).count()
    if fmt == "json":
        return spark.read.json(path).count()
    if fmt == "xml":
        return spark.read.format("xml").option("rowTag", load_config()["xml"]["row_tag"]).load(path).count()
    raise ValueError(f"Unsupported chunk format '{fmt}'")
