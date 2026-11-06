# vstone_framework - reusable components

Every notebook follows one pattern: **get context -> call framework functions -> show results**.
Logic lives in `src/vstone_framework/` (unit-tested in `tests/`); static definitions live in
`conf/vstone_config.json`; environment values are bundle variables passed as job parameters.

| Module | Responsibility | Used by | Tests |
|---|---|---|---|
| `config.py` | Load and validate `conf/vstone_config.json` | everything | `test_config.py` |
| `runtime.py` | Reads widgets (job + task parameters) into a Context: names, paths, typed task parameters | every notebook | `test_runtime.py` |
| `runner.py` | `run_per_table`: per-table try/log/continue, fail at the end | 03a, 03b, 03c | `test_runner.py` |
| `chunking.py` | Combine the 6 levels (`combine_levels`, `RegionLevel`), cut-offs, `with_chunk_no`, 4-file paths, re-run decision (`plan_action`), reconciliation per level, old-layout list | 02, 02b | `test_chunking.py` |
| `chunk_io.py` | Write a chunk as exactly ONE CSV/JSON/XML file (`write_single_file`), read it back to verify | 02 | `test_chunk_io.py` |
| `manifest.py` | `ops.chunk_manifest` DDL, lookup, MERGE upsert | 02 | (via job runs) |
| `schema_checks.py` | Header vs contract comparison | 01, 02, 03a | `test_schema_checks.py` |
| `bronze.py` | Catalogue of the 6 Bronze tables (4 chunk files + 2 crosswalks), picked by `chunk_no`; DDL with comments; COPY INTO SQL; schemas; header ordering; incremental file detection | 03a-03z, DLT | `test_bronze_sql.py`, `test_bronze_sources.py` |
| `audit.py` | `with_audit_columns` transform (load_dt, source, source_file, batch_id) | all loaders | `test_audit.py` |
| `metadata.py` | Column comments generated from `DataDictionary.csv` | all loaders | `test_metadata.py` |
| `quality.py` | Sanity checks: reconciliation, null keys, duplicates, audit, rescued data, column contract | 03z | `test_quality.py` |
| `ops.py` | `ops.pipeline_run_log`, `ops.sanity_check_results` | all loaders | (via job runs) |
| `headers.py` | snake_case header rule as Python function, **Python UDF** and **pandas UDF**; validated mapping | 04a, 04b | `test_headers.py` |
| `silver.py` | Silver steps: union Bronze sources, quality rules + quarantine, typing, empty-row removal, dedupe, finalise, reference cleaning, reconciliation | 04a, 04b, 04z | `test_silver.py` |
| `business_rules.py` | Metric catalogue (units, currency, scale, precision), value normalisation, date keys, region codes, unpivot | 05a | `test_business_rules.py` |
| `regions.py` | Region standardisation: names and hierarchy from crosswalks | 05a | `test_regions.py` |
| `features.py` | Silver features with gap-safe lags | 05b | `test_features.py` |
| `conformed_schema.py` | Column names, types and comments of the Day 5 tables | 05a, 05b | (via job runs) |
| `uc.py` | Unity Catalog tags/comments, level field contract | all loaders | (via job runs) |
| `gold.py` | Gold transformations: dimension snapshots, current rows of SCD2 tables, fact, top-10 / market trend / state sales aggregates, glossary rows, Gold checks | Gold DLT, 06b, 06z | `test_gold.py`, `test_gold_on_real_sample.py` |
| `gold_schema.py` | Gold columns, comments, PRIMARY/FOREIGN KEYs, DLT schema strings, generated ER diagram | Gold DLT, 06z, docs | `test_gold.py` |

Component-integration tests on real-data fixtures: `tests/integration/test_bronze_on_real_sample.py`,
`tests/integration/test_silver_on_real_sample.py`, `tests/integration/test_conformed_on_real_sample.py`.
Business rules: `docs/silver_business_rules.md`. UDF details: `docs/udf_header_standardization.md`.
