"""Unit tests for the COPY INTO and CREATE TABLE builders."""
from vstone_framework.bronze import CROSSWALK_COLUMNS, bronze_sources, copy_into_sql, create_table_ddl


def test_copy_into_loads_chunk1_file_and_the_two_crosswalks():
    srcs = bronze_sources(["copy_into"])
    assert [(s.table, s.file_name) for s in srcs] == [
        ("zillow_ts_chunk1_csv", "zillow_time_series_chunk1.csv"),
        ("county_crosswalk", "CountyCrossWalk_Zillow.csv"), ("city_crosswalk", "cities_crosswalk.csv")]


def test_copy_into_pattern_can_target_one_file():
    sql = copy_into_sql("c.b.county_crosswalk", "/Volumes/c/landing/raw", ["FIPS"], "copy_into_csv", "1",
                        pattern="CountyCrossWalk_Zillow.csv")
    assert "PATTERN = 'CountyCrossWalk_Zillow.csv'" in sql and "FROM '/Volumes/c/landing/raw'" in sql


def test_copy_into_sql_has_audit_columns_and_filters_csv_files():
    sql = copy_into_sql("vstone.bronze.zillow_ts_chunk1_csv", "/Volumes/vstone/landing/chunks/chunk1_csv",
                        ["Date", "RegionName", "ZHVI_AllHomes"], "copy_into_csv", "42")
    assert sql.startswith("COPY INTO vstone.bronze.zillow_ts_chunk1_csv")
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
