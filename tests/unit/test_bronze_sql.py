"""Unit tests for the COPY INTO and CREATE TABLE builders."""
from vstone_framework.bronze import CROSSWALK_COLUMNS, copy_into_sources, copy_into_sql, create_table_ddl


def test_copy_into_sources_cover_six_levels_and_two_crosswalks():
    tables = [s.table for s in copy_into_sources()]
    assert tables == ["state_ts_csv", "metro_ts_csv", "county_ts_csv", "city_ts_csv", "neighborhood_ts_csv",
                      "zip_ts_csv", "county_crosswalk", "city_crosswalk"]
    assert all(s.folder.startswith("csv_initial/") for s in copy_into_sources())


def test_copy_into_sql_has_audit_columns_and_filters_csv_files():
    sql = copy_into_sql("vstone.bronze.zip_ts_csv", "/Volumes/vstone/landing/chunks/csv_initial/zip",
                        ["Date", "RegionName", "ZHVI_AllHomes"], "copy_into_csv", "42")
    assert sql.startswith("COPY INTO vstone.bronze.zip_ts_csv")
    for part in ["`Date`", "`RegionName`", "`ZHVI_AllHomes`", "current_timestamp() AS load_dt",
                 "'copy_into_csv' AS source", "_metadata.file_path AS source_file", "'42' AS batch_id",
                 "FILEFORMAT = CSV", "PATTERN = '*.csv'", "'header' = 'true'", "'inferSchema' = 'false'",
                 "'mergeSchema' = 'false'"]:
        assert part in sql, part


def test_create_table_ddl_comments_every_column_and_escapes_quotes():
    ddl = create_table_ddl("vstone.bronze.city_crosswalk", CROSSWALK_COLUMNS["city_crosswalk"],
                           {"City": "City's name"}, "Bronze: city crosswalk")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS vstone.bronze.city_crosswalk")
    assert "`City` STRING COMMENT 'City\\'s name'" in ddl
    for col in ["load_dt TIMESTAMP", "source STRING", "source_file STRING", "batch_id STRING"]:
        assert col in ddl
    assert ddl.count("COMMENT") == len(CROSSWALK_COLUMNS["city_crosswalk"]) + 4 + 1
