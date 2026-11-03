# vstone_framework - reusable components

Every notebook follows one pattern: **get context -> call framework functions -> show results**.
Logic lives in `src/vstone_framework/` (unit-tested in `tests/`); static definitions live in
`conf/vstone_config.json`; environment values are bundle variables passed as job parameters.

| Module | Responsibility | Used by | Tests |
|---|---|---|---|
| `config.py` | Load and validate `conf/vstone_config.json` | everything | `test_config.py` |
| `runtime.py` | `get_context(dbutils)`: widgets, catalog/schema names, Volume paths | every notebook | `test_runtime.py` |
| `runner.py` | `run_per_table`: per-table try/log/continue, fail at the end | 03a, 03b, 03c | `test_runner.py` |
| `chunking.py` | Cut-offs, `with_chunk_no` transform, re-run decision (`plan_action`), reconciliation | 02, 02b | `test_chunking.py` |
| `chunk_io.py` | Write a chunk as CSV/JSON/XML, read it back to verify | 02 | `test_chunk_io.py` |
| `manifest.py` | `ops.chunk_manifest` DDL, lookup, MERGE upsert | 02 | (via job runs) |
| `schema_checks.py` | Header vs contract comparison | 01, 02, 03a | `test_schema_checks.py` |
| `bronze.py` | Catalogue of the 26 Bronze tables; DDL with comments; COPY INTO SQL; schemas; header ordering; incremental file detection | 03a-03z, DLT | `test_bronze_sql.py`, `test_bronze_sources.py` |
| `audit.py` | `with_audit_columns` transform (load_dt, source, source_file, batch_id) | all loaders | `test_audit.py` |
| `metadata.py` | Column comments generated from `DataDictionary.csv` | all loaders | `test_metadata.py` |
| `quality.py` | Sanity checks: reconciliation, null keys, duplicates, audit, rescued data, column contract | 03z | `test_quality.py` |
| `ops.py` | `ops.pipeline_run_log`, `ops.sanity_check_results` | all loaders | (via job runs) |
| `uc.py` | Unity Catalog tags/comments, level field contract | all loaders | (via job runs) |

Component-integration tests on real-data fixtures: `tests/integration/test_bronze_on_real_sample.py`.
