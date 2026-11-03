"""Unit tests for writing / reading chunk files (CSV and JSON locally; XML needs Databricks)."""
import pytest

from vstone_framework.chunk_io import read_back_count, write_chunk


@pytest.mark.parametrize("fmt, ext", [("csv", ".csv"), ("json", ".json")])
def test_write_then_read_back(spark, tmp_path, fmt, ext):
    df = spark.createDataFrame([(f"2010-01-{d:02d}", str(d)) for d in range(1, 21)], "Date string, RegionName string")
    path = str(tmp_path / fmt)
    write_chunk(df, fmt, path, n_rows=20, rows_per_file=8)          # 20 rows / 8 per file -> 3 files
    files = [p for p in (tmp_path / fmt).iterdir() if p.name.endswith(ext)]
    assert len(files) == 3
    assert read_back_count(spark, fmt, path) == 20


def test_unknown_format_rejected(spark, tmp_path):
    df = spark.createDataFrame([("x",)], "a string")
    with pytest.raises(ValueError, match="Unsupported"):
        write_chunk(df, "parquet", str(tmp_path / "p"), 1, 10)
    with pytest.raises(ValueError, match="Unsupported"):
        read_back_count(spark, "parquet", str(tmp_path / "p"))
