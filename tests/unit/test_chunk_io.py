"""Unit tests for writing the chunk files: each chunk must land as exactly ONE file (CSV, JSON locally;
XML needs Databricks' native XML support)."""
import os

import pytest

from vstone_framework.chunk_io import LocalFs, read_back_count, write_single_file


@pytest.mark.parametrize("fmt", ["csv", "json"])
def test_write_single_file_lands_exactly_one_named_file(spark, tmp_path, fmt):
    df = spark.createDataFrame([(f"2010-01-{d:02d}", str(d)) for d in range(1, 21)], "Date string, RegionName string")
    df = df.repartition(4)                                           # several partitions in, one file out
    final = str(tmp_path / "chunks" / f"chunk_{fmt}" / f"zillow_chunk.{fmt}")
    staging = str(tmp_path / "chunks" / "_staging" / f"chunk_{fmt}")
    write_single_file(df, fmt, staging, final, LocalFs())
    assert os.listdir(os.path.dirname(final)) == [os.path.basename(final)]       # exactly one file
    assert not os.path.exists(staging)                                           # staging removed
    assert read_back_count(spark, fmt, final) == 20


def test_rewrite_replaces_the_old_file(spark, tmp_path):
    final = str(tmp_path / "c" / "f.csv")
    for n in (5, 3):
        df = spark.createDataFrame([(str(i),) for i in range(n)], "a string")
        write_single_file(df, "csv", str(tmp_path / "s"), final, LocalFs())
    assert os.listdir(tmp_path / "c") == ["f.csv"] and read_back_count(spark, "csv", final) == 3


def test_unknown_format_rejected(spark, tmp_path):
    df = spark.createDataFrame([("x",)], "a string")
    with pytest.raises(ValueError, match="Unsupported"):
        write_single_file(df, "parquet", str(tmp_path / "s"), str(tmp_path / "c" / "f.parquet"), LocalFs())
    with pytest.raises(ValueError, match="Unsupported"):
        read_back_count(spark, "parquet", str(tmp_path / "p"))
