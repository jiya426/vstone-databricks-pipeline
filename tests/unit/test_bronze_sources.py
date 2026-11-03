"""Unit tests for the Bronze table catalogue and helpers used by all four ingestion methods."""
from pyspark.sql.types import StringType

from vstone_framework.bronze import (RESCUED_COL, bronze_sources, contract_columns, new_files, normalize_path,
                                     schema_ddl, string_schema, table_columns)

FIELDS = {"zip": ["Date", "RegionName", "ZHVI_AllHomes"]}


def test_26_bronze_tables_four_per_level_plus_crosswalks():
    srcs = bronze_sources()
    assert len(srcs) == 26
    zip_tables = sorted(s.table for s in srcs if s.entity == "zip")
    assert zip_tables == ["zip_ts_csv", "zip_ts_csv_dlt", "zip_ts_json", "zip_ts_xml"]
    assert {s.method for s in bronze_sources(["autoloader"])} == {"autoloader"}
    assert len(bronze_sources(["pyspark_xml"])) == 6


def test_methods_map_to_audit_source_and_chunk():
    by_method = {s.method: (s.chunk_no, s.source, s.file_format, s.has_rescued) for s in bronze_sources()
                 if s.entity == "zip"}
    assert by_method == {"copy_into": (1, "copy_into_csv", "csv", False), "dlt": (2, "dlt_csv", "csv", True),
                         "autoloader": (3, "autoloader_json", "json", True),
                         "pyspark_xml": (4, "pyspark_xml", "xml", True)}


def test_table_columns_order_contract_rescued_audit():
    src = [s for s in bronze_sources(["autoloader"]) if s.entity == "zip"][0]
    assert table_columns(src, FIELDS) == ["Date", "RegionName", "ZHVI_AllHomes", RESCUED_COL,
                                          "load_dt", "source", "source_file", "batch_id"]
    assert contract_columns(src, FIELDS) == FIELDS["zip"]


def test_string_schema_is_all_strings():
    schema = string_schema(["Date", "ZHVI_AllHomes"])
    assert [f.name for f in schema] == ["Date", "ZHVI_AllHomes"]
    assert all(isinstance(f.dataType, StringType) and f.nullable for f in schema)


def test_schema_ddl_for_dlt_has_comments_and_rescued_column():
    ddl = schema_ddl(["Date"], {"Date": "Month-end date"}, rescued=True)
    assert "`Date` STRING COMMENT 'Month-end date'" in ddl
    assert f"{RESCUED_COL} STRING COMMENT" in ddl
    assert "load_dt TIMESTAMP COMMENT" in ddl


def test_new_files_is_incremental_and_ignores_dbfs_prefix():
    available = ["dbfs:/Volumes/v/xml/zip/part-0.xml", "dbfs:/Volumes/v/xml/zip/part-1.xml"]
    loaded = ["/Volumes/v/xml/zip/part-0.xml", None]
    assert new_files(available, loaded) == ["/Volumes/v/xml/zip/part-1.xml"]
    assert new_files(available, [normalize_path(p) for p in available]) == []      # re-run -> nothing new
