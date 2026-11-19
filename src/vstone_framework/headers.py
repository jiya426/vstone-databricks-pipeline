"""Header standardisation (PDF Day 4: "Develop User-Defined Functions (UDFs) using Pandas/Python UDF
to standardize DataFrame/Table headers").

Zillow headers mix CamelCase, acronyms, digits and underscores:
    ZHVI_AllHomes                                  -> zhvi_all_homes
    MedianListingPricePerSqft_1Bedroom             -> median_listing_price_per_sqft_1_bedroom
    ZHVI_1bedroom                                  -> zhvi_1_bedroom
    MedianRentalPrice_MultiFamilyResidence5PlusUnits -> median_rental_price_multi_family_residence_5_plus_units
    \\ufeffCountyName (UTF-8 BOM)                   -> county_name
    CBSACode                                       -> cbsa_code

The rule is one pure Python function, `to_snake_case`, exposed three ways:
  * plain Python (unit tests, driver-side mapping)
  * a Python UDF        `snake_case_udf`         (row-at-a-time)
  * a pandas UDF        `snake_case_pandas_udf`  (vectorised with Apache Arrow - faster on large inputs)

Headers are metadata, not rows, so `header_mapping` puts the column names into a one-column DataFrame,
runs the chosen UDF over it, checks the result is unique and valid, and returns {old: new}.
"""
from __future__ import annotations

import re
import sys
from typing import Dict, List, Sequence

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

_VALID = re.compile(r"^[a-z_][a-z0-9_]*$")

# UDFs run in Python worker processes that cannot import vstone_framework (the notebook only adds src/ to the
# DRIVER's sys.path). Registering this module "by value" makes Spark ship the function code itself with the UDF.
try:
    from pyspark import cloudpickle as _cloudpickle
    _cloudpickle.register_pickle_by_value(sys.modules[__name__])
except Exception:  # noqa: BLE001 - older cloudpickle: falls back to by-reference pickling
    pass


def to_snake_case(name: str) -> str:
    """Convert any header to lower snake_case (see module docstring for examples)."""
    if name is None:
        return None
    s = str(name).replace("﻿", "").strip()
    s = re.sub(r"[^0-9A-Za-z]+", "_", s)                 # spaces, dots, hyphens -> _
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)     # ZHVIPer -> ZHVI_Per, CBSACode -> CBSA_Code
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)           # allHomes -> all_Homes
    s = re.sub(r"([A-Za-z])([0-9])", r"\1_\2", s)        # Residence5 -> Residence_5
    s = re.sub(r"([0-9])([A-Za-z])", r"\1_\2", s)        # 1Bedroom -> 1_Bedroom
    s = re.sub(r"_+", "_", s).strip("_").lower()
    if s and s[0].isdigit():                              # SQL identifiers must not start with a digit
        s = f"c_{s}"
    return s


# Python UDF: Spark calls the function once per row
snake_case_udf = F.udf(to_snake_case, StringType())


# pandas UDF: Spark sends batches of rows as a pandas Series (Arrow), one call per batch
@F.pandas_udf(StringType())
def snake_case_pandas_udf(names: pd.Series) -> pd.Series:
    return names.map(to_snake_case)


def validate_mapping(mapping: Dict[str, str]) -> None:
    """New names must be valid identifiers and unique (two headers must never collapse into one)."""
    invalid = [v for v in mapping.values() if not _VALID.match(v or "")]
    if invalid:
        raise ValueError(f"Invalid standardised names: {invalid}")
    seen: Dict[str, str] = {}
    for old, new in mapping.items():
        if new in seen:
            raise ValueError(f"Header collision: '{seen[new]}' and '{old}' both become '{new}'")
        seen[new] = old


def header_mapping(spark, columns: Sequence[str], method: str = "pandas") -> Dict[str, str]:
    """Run the UDF over the header names and return a validated {original: standardised} mapping."""
    udf = {"pandas": snake_case_pandas_udf, "python": snake_case_udf}[method]
    names = spark.createDataFrame([(c,) for c in columns], "original STRING")
    mapping = {r.original: r.standardised for r in names.select("original", udf("original").alias("standardised")).collect()}
    validate_mapping(mapping)
    return mapping


def standardize_headers(mapping: Dict[str, str]):
    """Transformation for DataFrame.transform(): renames columns using a mapping from `header_mapping`."""
    def _transform(df: DataFrame) -> DataFrame:
        return df.select(*[F.col(f"`{c}`").alias(mapping.get(c, c)) for c in df.columns])
    return _transform


def mapping_rows(mapping: Dict[str, str], table: str) -> List[tuple]:
    """Rows for the silver.column_name_map documentation table."""
    return [(table, old, new) for old, new in mapping.items()]
