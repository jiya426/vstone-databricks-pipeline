# UDF documentation - header standardisation (PDF Day 4)

**Module:** `src/vstone_framework/headers.py` | **Used by:** `04a_silver_time_series`, `04b_silver_reference`
**Tests:** `tests/unit/test_headers.py` (12 naming cases, Python vs pandas UDF agreement, collisions)

## Purpose
Zillow source headers mix CamelCase, acronyms, digits, underscores and a UTF-8 byte-order mark. Silver and Gold
use lower `snake_case`, so every name is standardised once, consistently, and recorded in
`vstone.silver.column_name_map` (source name -> Silver name) for lineage.

## Rule (`to_snake_case`)
| Step | Regex | Example |
|---|---|---|
| Remove BOM, trim | - | `﻿CountyName` -> `CountyName` |
| Non-alphanumeric -> `_` | `[^0-9A-Za-z]+` | `Median Price-USD` -> `Median_Price_USD` |
| Split acronym from word | `([A-Z]+)([A-Z][a-z])` | `ZHVIPerSqft` -> `ZHVI_PerSqft` |
| Split camelCase | `([a-z])([A-Z])` | `AllHomes` -> `All_Homes` |
| Split letter/digit | `([A-Za-z])([0-9])` | `Residence5` -> `Residence_5` |
| Split digit/letter | `([0-9])([A-Za-z])` | `1Bedroom` -> `1_Bedroom` |
| Collapse `_`, lower-case | `_+` | `zhvi_per_sqft_all_homes` |
| Leading digit -> prefix `c_` | - | `5YearChange` -> `c_5_year_change` |

## Three forms of the same rule
| Form | Object | How Spark runs it | When to use |
|---|---|---|---|
| Python function | `to_snake_case(name)` | plain Python on the driver | unit tests, small lists |
| Python UDF | `snake_case_udf` | one Python call **per row** | simple, small data |
| pandas UDF | `snake_case_pandas_udf` | one call **per batch**, data moved with Apache Arrow as a pandas Series | larger data: far fewer Python calls (configured default: `silver.header_udf = "pandas"`) |

`header_mapping(spark, columns, method)` puts the header names in a one-column DataFrame, applies the chosen UDF,
validates the result (valid identifiers, **no two headers collapsing into one name**) and returns `{old: new}`.
`standardize_headers(mapping)` is a `DataFrame.transform` step that renames the columns.

All 112 real names (96 metrics + keys + both crosswalks) map to unique, valid names.

## Engineering note: UDFs and worker processes
UDF code runs in Python **worker** processes. Notebooks add `src/` to the **driver's** `sys.path` only, so a
UDF referencing `vstone_framework` failed with `ModuleNotFoundError` in tests. Fix: `headers.py` registers itself
with `cloudpickle.register_pickle_by_value`, so Spark ships the function code with the UDF and workers need no import.
