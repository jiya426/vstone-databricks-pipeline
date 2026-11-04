"""Unit tests for region standardisation (names and hierarchy from the crosswalks)."""
from vstone_framework.config import load_config
from vstone_framework.regions import REGION_COLUMNS, build_region_reference

RULES = load_config()["business_rules"]


def test_region_reference(spark):
    regions = spark.createDataFrame(
        [("state", "NewYork"), ("state", "UnitedStates"), ("metro", "35620"), ("metro", "United_States"),
         ("county", "01003"), ("city", "aberdeenharfordmd"), ("city", "notacityslug"), ("zip", "01001")],
        "level string, region_code string")
    county_cw = spark.createDataFrame(
        [("Baldwin", "Alabama", "01", "003", "Daphne, AL", "Daphne-Fairhope-Foley, AL", "01003", "19300"),
         ("Kings", "New York", "36", "047", "New York, NY", "New York-Newark-Jersey City", "36047", "35620"),
         ("Harford", "Maryland", "24", "025", "Baltimore, MD", "Baltimore-Columbia-Towson, MD", "24025", "12580")],
        "county_name string, state_name string, state_fips string, county_fips string, metro_name_zillow string, "
        "cbsa_name string, fips string, cbsa_code string")
    city_cw = spark.createDataFrame([("aberdeenharfordmd", "Aberdeen", "Harford", "MD")],
                                    "unique_city_id string, city string, county string, state string")
    out = {r.region_key: r for r in build_region_reference(regions, county_cw, city_cw, RULES).collect()}
    assert list(build_region_reference(regions, county_cw, city_cw, RULES).columns) == REGION_COLUMNS
    assert (out["state:NewYork"].state_name, out["state:NewYork"].state_code) == ("New York", "NY")
    assert out["state:UnitedStates"].region_type == "national" and out["state:UnitedStates"].is_resolved
    assert out["metro:35620"].metro_name == "New York, NY"
    c = out["county:01003"]
    assert (c.region_name, c.county_fips, c.metro_cbsa_code, c.state_code) == ("Baldwin, AL", "01003", "19300", "AL")
    ab = out["city:aberdeenharfordmd"]
    assert (ab.region_name, ab.county_fips, ab.metro_name, ab.state_name) == ("Aberdeen, MD", "24025", "Baltimore, MD", "Maryland")
    assert not out["city:notacityslug"].is_resolved and out["city:notacityslug"].region_name == "notacityslug"
    assert not out["zip:01001"].is_resolved
