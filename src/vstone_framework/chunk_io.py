"""Writing and reading the chunk files (used by the Data Chunking job).

Spark always writes a FOLDER of part files. The trainer's rule is "land exactly 4 files", so each chunk
is written with ONE partition into a staging folder, the single part file is copied to its final name
(e.g. chunk3_json/zillow_time_series_chunk3.json), and the staging folder is deleted.

`fs` is anything with ls / cp / rm / mkdirs (dbutils.fs on Databricks, LocalFs in tests).
"""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame

from .config import load_config


def _writer(df: DataFrame, fmt: str, path: str) -> None:
    xml = load_config()["xml"]
    writer = df.repartition(1).write.mode("overwrite")          # ONE partition -> ONE part file
    if fmt == "csv":
        writer.option("header", True).csv(path)
    elif fmt == "json":
        writer.json(path)                                       # null fields are omitted by the JSON writer
    elif fmt == "xml":
        writer.format("xml").option("rootTag", xml["root_tag"]).option("rowTag", xml["row_tag"]).save(path)
    else:
        raise ValueError(f"Unsupported chunk format '{fmt}'")


def part_files(fs, folder: str) -> List[str]:
    """Data files Spark wrote into a folder (ignores _SUCCESS, _committed_*, .crc files)."""
    return sorted(f.path for f in fs.ls(folder)
                  if f.name.startswith("part-") and not f.name.endswith(".crc"))


def write_single_file(df: DataFrame, fmt: str, staging: str, final_file: str, fs, clear_folder: bool = True) -> str:
    """Write `df` as exactly one file named `final_file`.
    clear_folder=True (chunk files): the folder then holds only that file.
    clear_folder=False (late-arriving batches): earlier files in the folder are kept."""
    _writer(df, fmt, staging)
    parts = part_files(fs, staging)
    if len(parts) != 1:
        raise AssertionError(f"Expected 1 part file in {staging}, found {len(parts)}: {parts}")
    folder = final_file.rsplit("/", 1)[0]
    if clear_folder:
        fs.rm(folder, True)          # remove any older file so the folder holds exactly one file
    fs.mkdirs(folder)
    fs.cp(parts[0], final_file)
    fs.rm(staging, True)
    staging_root = staging.rstrip("/").rsplit("/", 1)[0]
    if not fs.ls(staging_root):              # last chunk written: remove the empty staging root too
        fs.rm(staging_root, True)
    return final_file


def read_back_count(spark, fmt: str, path: str) -> int:
    """Count the rows actually written - proves the chunk is readable in its format."""
    if fmt == "csv":
        return spark.read.option("header", True).csv(path).count()
    if fmt == "json":
        return spark.read.json(path).count()
    if fmt == "xml":
        return spark.read.format("xml").option("rowTag", load_config()["xml"]["row_tag"]).load(path).count()
    raise ValueError(f"Unsupported chunk format '{fmt}'")


# ------------------------------------------------------------------ local file system (tests)
@dataclass(frozen=True)
class FileInfo:
    path: str
    name: str


class LocalFs:
    """Same calls as dbutils.fs, on the local disk - lets the tests run write_single_file."""

    def ls(self, folder: str) -> List[FileInfo]:
        return [FileInfo(os.path.join(folder, n), n) for n in sorted(os.listdir(folder))]

    def rm(self, path: str, recurse: bool = False) -> bool:
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)
        return True

    def mkdirs(self, path: str) -> bool:
        os.makedirs(path, exist_ok=True)
        return True

    def cp(self, src: str, dst: str) -> bool:
        shutil.copyfile(src, dst)
        return True
