"""Delta Live Tables (Lakeflow Declarative Pipelines) - Bronze: the chunk 2 CSV file.

Creates ONE streaming table, `<catalog>.<bronze_schema>.<table of chunk 2>` (zillow_ts_chunk2_csv_dlt),
reading the single chunk 2 file with Auto Loader inside the pipeline.

Code parameterization: a DLT pipeline has no widgets, so every value comes from the pipeline
`configuration` block in resources/pipeline_bronze_csv_dlt.yml (filled from bundle variables):
  vstone.catalog, vstone.landing_schema, vstone.raw_volume, vstone.chunks_volume,
  vstone.chunk_no, vstone.framework_path
The table name, folder and audit `source` value are looked up in conf/vstone_config.json by chunk_no.

Why DLT: declarative tables, built-in data-quality expectations with metrics in the pipeline UI,
and lineage in Unity Catalog. Expectations here WARN (rows are kept): Bronze stores raw data; Silver quarantines.
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
from vstone_framework.bronze import (RESCUED_COL, contract_columns, first_csv_header,  # noqa: E402
                                     header_ordered_columns, require_method, schema_ddl, source_for_chunk,
                                     string_schema)
from vstone_framework.chunking import LEVEL_COL, LEVELS  # noqa: E402
from vstone_framework.config import load_config  # noqa: E402
from vstone_framework.metadata import column_comments, load_data_dictionary  # noqa: E402
from vstone_framework.uc import load_level_fields  # noqa: E402

# ---- pipeline parameters (configuration block) ----
cfg = load_config()
catalog = spark.conf.get("vstone.catalog")
landing = spark.conf.get("vstone.landing_schema")
chunk_no = int(spark.conf.get("vstone.chunk_no"))
raw_path = f"/Volumes/{catalog}/{landing}/{spark.conf.get('vstone.raw_volume')}"
chunks_root = f"/Volumes/{catalog}/{landing}/{spark.conf.get('vstone.chunks_volume')}"
batch_id = spark.conf.get("pipelines.id", cfg["defaults"]["run_id"])   # pipeline id identifies the loader

src = require_method(source_for_chunk(chunk_no), "dlt")
level_fields = load_level_fields(f"{raw_path}/{cfg['metadata']['fields_per_level']}")
dictionary = load_data_dictionary(f"{raw_path}/{cfg['metadata']['data_dictionary']}")
path = src.landing_path(chunks_root, raw_path)
# CSV schemas are applied by position: take the order from the file header, validated against the contract
cols = header_ordered_columns(first_csv_header(path), contract_columns(src, level_fields))
levels_sql = ", ".join(f"'{lvl}'" for lvl in LEVELS)

EXPECTATIONS = {
    "date_present": "Date IS NOT NULL",
    "date_is_valid": "try_to_timestamp(Date, 'yyyy-MM-dd') IS NOT NULL",
    "region_present": "RegionName IS NOT NULL",
    "region_level_valid": f"{LEVEL_COL} IN ({levels_sql})",
    "no_rescued_data": f"{RESCUED_COL} IS NULL",
}


@dlt.table(
    name=src.table,
    comment=src.description,
    schema=schema_ddl(cols, column_comments(cols, dictionary), rescued=True),
    table_properties={"quality": "bronze", "project": cfg["project"], "entity": src.entity,
                      "chunk_no": str(chunk_no)},
)
@dlt.expect_all(EXPECTATIONS)
def bronze_chunk_csv_dlt():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("rescuedDataColumn", RESCUED_COL)
            .schema(string_schema(cols))
            .load(path)
            .select(*[F.col(f"`{c}`") for c in cols], F.col(RESCUED_COL), F.col("_metadata"))
            .transform(with_audit_columns(src.source, batch_id))
            .drop("_metadata"))
