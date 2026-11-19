"""Unit tests for the Bronze table catalogue: 4 landed files -> 4 Bronze tables, + 2 crosswalks."""
import pytest
from pyspark.sql.types import StringType

from vstone_framework.bronze import (RESCUED_COL, bronze_sources, contract_columns, new_files, normalize_path,
                                     reference_sources, require_method, schema_ddl, source_for_chunk, string_schema,
                                     table_columns, time_series_sources)
from vstone_framework.chunking import LEVEL_COL

FIELDS = {"state": ["Date", "RegionName", "ZHVI_AllHomes"], "metro": ["Date", "RegionName", "ZRI_AllHomes"],
          "county": ["Date", "RegionName"], "city": ["Date", "RegionName"], "neighborhood": ["Date", "RegionName"],
          "zip": ["RegionName", "Date", "ZHVI_AllHomes", "MedianListingPrice_AllHomes"]}


def test_four_time_series_tables_one_per_file_plus_two_crosswalks():
    ts = time_series_sources()
    assert [s.table for s in ts] == ["zillow_ts_chunk1_csv", "zillow_ts_chunk2_csv_dlt", "zillow_ts_chunk3_json",
                                     "zillow_ts_chunk4_xml"]
    assert [s.table for s in reference_sources()] == ["county_crosswalk", "city_crosswalk"]
    assert len(bronze_sources()) == 6
    assert [s.table for s in bronze_sources(["copy_into"])] == ["zillow_ts_chunk1_csv", "county_crosswalk",
                                                                  "city_crosswalk"]


def test_each_chunk_has_its_own_method_source_and_file():
    by_chunk = {s.chunk_no: (s.method, s.source, s.file_format, s.has_rescued) for s in time_series_sources()}
    assert by_chunk == {1: ("copy_into", "copy_into_csv", "csv", False), 2: ("dlt", "dlt_csv", "csv", True),
                        3: ("autoloader", "autoloader_json", "json", True), 4: ("pyspark_xml", "pyspark_xml", "xml", True)}
    assert source_for_chunk(3).file_name == "zillow_time_series_chunk3.json"
    assert source_for_chunk(3).key_columns == ("Date", "RegionName", LEVEL_COL)


def test_task_parameter_guard_rejects_wrong_chunk_for_a_notebook():
    assert require_method(source_for_chunk(4), "pyspark_xml").chunk_no == 4
    with pytest.raises(ValueError, match="configured for 'csv'|configured for 'copy_into'"):
        require_method(source_for_chunk(1), "autoloader")
    with pytest.raises(ValueError, match="No chunk 7"):
        source_for_chunk(7)


def test_landing_paths_chunks_volume_for_time_series_raw_volume_for_crosswalks():
    assert source_for_chunk(2).landing_path("/V/chunks", "/V/raw") == "/V/chunks/chunk2_csv"
    assert reference_sources()[0].landing_path("/V/chunks", "/V/raw") == "/V/raw"
    assert reference_sources()[0].file_name == "CountyCrossWalk_Zillow.csv"


def test_all_four_tables_share_the_combined_contract():
    contracts = {tuple(contract_columns(s, FIELDS)) for s in time_series_sources()}
    assert len(contracts) == 1
    cols = list(contracts.pop())
    assert cols[:3] == ["Date", "RegionName", LEVEL_COL]
    assert set(cols[3:]) == {"ZHVI_AllHomes", "ZRI_AllHomes", "MedianListingPrice_AllHomes"}


def test_table_columns_order_contract_rescued_audit():
    src = source_for_chunk(3)
    cols = table_columns(src, FIELDS)
    assert cols[-5:] == [RESCUED_COL, "load_dt", "source", "source_file", "batch_id"]


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
    available = ["dbfs:/Volumes/v/chunk4_xml/a.xml", "dbfs:/Volumes/v/chunk4_xml/b.xml"]
    loaded = ["/Volumes/v/chunk4_xml/a.xml", None]
    assert new_files(available, loaded) == ["/Volumes/v/chunk4_xml/b.xml"]
    assert new_files(available, [normalize_path(p) for p in available]) == []      # re-run -> nothing new
