"""Delta Live Tables (Lakeflow Declarative Pipelines) - Day 3: CSV chunk 2 into Bronze.

Creates one streaming table per level, `vstone.bronze.<level>_ts_csv_dlt`, reading
`chunks/csv_incremental/<level>/` with Auto Loader inside the pipeline.

Why DLT: declarative tables, automatic dependency handling, built-in data-quality
expectations with metrics in the pipeline UI, and lineage in Unity Catalog.
Expectations here WARN (rows are kept): Bronze stores raw data; Silver quarantines.

Pipeline configuration keys (set in resources/pipeline_bronze_csv_dlt.yml):
  vstone.catalog, vstone.landing_schema, vstone.raw_volume, vstone.chunks_volume, vstone.framework_path
"""
import os
import sys

import dlt
from pyspark.sql import functions as F

# Make src/ importable: configured path first, then paths relative to this file / working dir
_candidates = [spark.conf.get("vstone.framework_path", ""), os.path.join(os.getcwd(), ".."), os.getcwd()]
try:
    _candidates.insert(1, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
except NameError:
    pass
_src = next((p for p in _candidates if p and os.path.isdir(os.path.join(p, "vstone_framework"))), None)
if _src is None:
    raise ImportError(f"vstone_framework not found; tried {_candidates}. Check vstone.framework_path in the pipeline config.")
sys.path.insert(0, os.path.abspath(_src))
from vstone_framework.audit import with_audit_columns  # noqa: E402
from vstone_framework.bronze import (RESCUED_COL, bronze_sources, contract_columns, first_csv_header,  # noqa: E402
                                     header_ordered_columns, schema_ddl, string_schema)
from vstone_framework.config import load_config  # noqa: E402
from vstone_framework.metadata import column_comments, load_data_dictionary  # noqa: E402
from vstone_framework.uc import load_level_fields  # noqa: E402

cfg = load_config()
catalog = spark.conf.get("vstone.catalog")
landing = spark.conf.get("vstone.landing_schema")
raw_path = f"/Volumes/{catalog}/{landing}/{spark.conf.get('vstone.raw_volume')}"
chunks_root = f"/Volumes/{catalog}/{landing}/{spark.conf.get('vstone.chunks_volume')}"
batch_id = spark.conf.get("pipelines.id", "dlt_pipeline")   # pipeline id identifies the loader

level_fields = load_level_fields(f"{raw_path}/fields_per_level.json")
dictionary = load_data_dictionary(f"{raw_path}/DataDictionary.csv")

EXPECTATIONS = {
    "date_present": "Date IS NOT NULL",
    "date_is_valid": "try_to_timestamp(Date, 'yyyy-MM-dd') IS NOT NULL",
    "region_present": "RegionName IS NOT NULL",
    "no_rescued_data": f"{RESCUED_COL} IS NULL",
}


def define_table(src):
    path = f"{chunks_root}/{src.folder}"
    # CSV schemas are applied by position: take the order from the file header, validated against the contract
    cols = header_ordered_columns(first_csv_header(path), contract_columns(src, level_fields))

    @dlt.table(
        name=src.table,
        comment=src.description,
        schema=schema_ddl(cols, column_comments(cols, dictionary), rescued=True),
        table_properties={"quality": "bronze", "project": cfg["project"], "entity": src.entity},
    )
    @dlt.expect_all(EXPECTATIONS)
    def _bronze_table():
        return (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("rescuedDataColumn", RESCUED_COL)
                .schema(string_schema(cols))
                .load(path)
                .select(*[F.col(f"`{c}`") for c in cols], F.col(RESCUED_COL), F.col("_metadata"))
                .transform(with_audit_columns(src.source, batch_id))
                .drop("_metadata"))

    return _bronze_table


for _src in bronze_sources(["dlt"]):
    define_table(_src)
