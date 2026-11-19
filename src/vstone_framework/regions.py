"""Region standardisation: one reference row per (level, region_code) with human-readable names
and its place in the geography hierarchy (city -> county -> metro -> state), built from the Silver crosswalks.

| Level        | region_code        | Resolved from                                   |
|--------------|--------------------|-------------------------------------------------|
| state        | 'NewYork'          | crosswalk state names with spaces removed       |
| metro        | CBSA code '35620'  | county crosswalk (metro name, CBSA name)        |
| county       | FIPS '01003'       | county crosswalk (zero-padded FIPS)             |
| city         | slug               | city crosswalk, then county crosswalk for FIPS  |
| neighborhood | Zillow id          | no crosswalk in the dataset -> unresolved       |
| zip          | ZIP code           | no crosswalk in the dataset -> unresolved       |
National rows ('UnitedStates', 'United_States') get region_type = 'national'.
"""
from __future__ import annotations

from functools import reduce
from typing import Dict

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .business_rules import code_to_state_name, state_code_col, state_name_lookup

REGION_COLUMNS = ["level", "region_code", "region_key", "region_name", "region_type", "state_code", "state_name",
                  "county_fips", "county_name", "metro_cbsa_code", "metro_name", "city_name", "is_resolved"]


def _null():
    return F.lit(None).cast("string")


def _shape(df: DataFrame, **cols) -> DataFrame:
    """Select REGION_COLUMNS, filling any column not given with NULL."""
    exprs = {c: cols.get(c, _null()) for c in REGION_COLUMNS if c not in ("level", "region_code", "region_key")}
    return df.select("level", "region_code", F.concat_ws(":", "level", "region_code").alias("region_key"),
                     *[e.alias(c) for c, e in exprs.items()])


def build_region_reference(regions: DataFrame, county_cw: DataFrame, city_cw: DataFrame, rules: Dict) -> DataFrame:
    """`regions` has columns (level, region_code) - the distinct standardised codes found in the data."""
    national = rules["national_regions"]
    by_level = lambda lvl: regions.filter(F.col("level") == lvl)
    is_national = lambda lvl: F.col("region_code") == F.lit(national.get(lvl, "__none__"))
    region_type = lambda lvl: F.when(is_national(lvl), "national").otherwise("regional")

    # state
    states = by_level("state").join(state_name_lookup(county_cw), F.col("region_code") == F.col("compact"), "left")
    state = _shape(states,
                   region_name=F.when(is_national("state"), "United States").otherwise(F.coalesce("state_name", "region_code")),
                   region_type=region_type("state"), state_name=F.col("state_name"),
                   state_code=state_code_col(F.col("state_name"), rules),
                   is_resolved=(F.col("state_name").isNotNull() | is_national("state")))

    # metro (one name per CBSA)
    metro_lkp = county_cw.filter(F.col("cbsa_code").isNotNull()).groupBy("cbsa_code").agg(
        F.first("metro_name_zillow", ignorenulls=True).alias("metro_name"), F.first("cbsa_name", ignorenulls=True).alias("cbsa_name"))
    metros = by_level("metro").join(metro_lkp, F.col("region_code") == F.col("cbsa_code"), "left")
    metro = _shape(metros,
                   region_name=F.when(is_national("metro"), "United States").otherwise(F.coalesce("metro_name", "region_code")),
                   region_type=region_type("metro"), metro_cbsa_code=F.col("cbsa_code"), metro_name=F.col("metro_name"),
                   is_resolved=(F.col("metro_name").isNotNull() | is_national("metro")))

    # county
    cty = county_cw.select("fips", "county_name", "state_name", "cbsa_code", F.col("metro_name_zillow").alias("metro_name"))
    counties = by_level("county").join(cty, F.col("region_code") == F.col("fips"), "left")
    state_code = state_code_col(F.col("state_name"), rules)
    county = _shape(counties,
                    region_name=F.coalesce(F.concat_ws(", ", "county_name", state_code), F.col("region_code")),
                    region_type=F.lit("regional"), state_code=state_code, state_name=F.col("state_name"),
                    county_fips=F.col("fips"), county_name=F.col("county_name"),
                    metro_cbsa_code=F.col("cbsa_code"), metro_name=F.col("metro_name"),
                    is_resolved=F.col("fips").isNotNull())

    # city: city crosswalk, then county crosswalk (by county + state name) for FIPS and metro
    cc = city_cw.select("unique_city_id", "city", F.col("county").alias("c_county"), F.col("state").alias("c_state"))
    cities = (by_level("city").join(cc, F.col("region_code") == F.col("unique_city_id"), "left")
              .withColumn("c_state_name", code_to_state_name(F.col("c_state"), rules)))
    cty2 = county_cw.select(F.lower("county_name").alias("k_county"), F.col("state_name").alias("k_state"), "fips",
                            "cbsa_code", F.col("metro_name_zillow").alias("metro_name"))
    cities = cities.join(cty2, (F.lower("c_county") == F.col("k_county")) & (F.col("c_state_name") == F.col("k_state")), "left")
    cities = (cities.withColumn("_rn", F.row_number().over(Window.partitionBy("region_code").orderBy(F.col("fips").asc_nulls_last())))
              .filter("_rn = 1"))
    city = _shape(cities,
                  region_name=F.when(F.col("city").isNotNull(), F.concat_ws(", ", "city", "c_state")).otherwise(F.col("region_code")),
                  region_type=F.lit("regional"), state_code=F.col("c_state"), state_name=F.col("c_state_name"),
                  county_fips=F.col("fips"), county_name=F.col("c_county"), metro_cbsa_code=F.col("cbsa_code"),
                  metro_name=F.col("metro_name"), city_name=F.col("city"), is_resolved=F.col("city").isNotNull())

    # neighborhood and zip: no crosswalk in the dataset
    others = [_shape(by_level(lvl), region_name=F.col("region_code"), region_type=F.lit("regional"),
                     is_resolved=F.lit(False)) for lvl in ("neighborhood", "zip")]

    out = reduce(lambda a, b: a.unionByName(b), [state, metro, county, city] + others)
    return out.withColumn("is_resolved", F.col("is_resolved").cast("boolean"))
