# Day 2 Evidence - Bronze ingestion with COPY INTO

Branch: `feature/bronze-layer` (PR #2 into `dev`, merged after Day 3 as the PDF requires) | Date: 2026-11-03
Screenshots go in `docs/evidence/day2/`.

## 1. What was built
| Component | File | Purpose |
|---|---|---|
| COPY INTO notebook | `src/notebooks/03a_bronze_copy_into.py` | Chunk 1 CSV (6 levels) + 2 crosswalks into `vstone.bronze` |
| Sanity-check notebook | `src/notebooks/03z_bronze_sanity_checks.py` | 5 checks per Bronze table, results in `vstone.ops.sanity_check_results` |
| Audit columns | `src/vstone_framework/audit.py` | `load_dt`, `source`, `source_file`, `batch_id` |
| Metadata | `src/vstone_framework/metadata.py` | Column comments generated from `DataDictionary.csv` (96/96 metrics matched) |
| SQL builders | `src/vstone_framework/bronze.py` | `CREATE TABLE ... USING DELTA` with comments; `COPY INTO` with audit columns |
| Monitoring | `src/vstone_framework/ops.py` | `ops.pipeline_run_log`, `ops.sanity_check_results` |
| Job (DABs) | `resources/job_bronze_ingestion.yml` | `vstone_bronze_ingestion`: copy_into -> sanity checks, retries 0, email on failure |

## 2. Job runs
| Run | Result | Screenshot |
|---|---|---|
| 1 | SUCCESS - rows loaded per table below | `01_bronze_run1.png` |
| 2 (re-run) | SUCCESS - **0 new rows** in all 8 tables (COPY INTO skips loaded files) | `02_bronze_run2_zero_rows.png` |

| Bronze table | Files | Rows (run 1) | Chunk 1 rows in manifest |
|---|---|---|---|
| state_ts_csv | 1 | 5,773 | 5,773 |
| metro_ts_csv | 1 | 86,917 | 86,917 |
| county_ts_csv | 1 | 186,615 | 186,615 |
| city_ts_csv | 7 | 1,523,942 | 1,523,942 |
| neighborhood_ts_csv | 3 | 700,701 | 700,701 |
| zip_ts_csv | 8 | 1,802,455 | 1,802,455 |
| county_crosswalk | 1 | 3,144 | 3,144 |
| city_crosswalk | 1 | 25,341 | 25,341 |

## 3. Sanity checks (reconciliation, nulls, duplicates, audit, schema)
40 checks (8 tables x 5), all PASS. Example: `zip_ts_csv row_count_matches_source expected 1802455 actual 1802455 PASS`;
`state_ts_csv columns_match_contract expected 86 actual 86` (82 source + 4 audit columns).
Screenshot: `03_sanity_checks_pass.png`

## 4. Metadata / discoverability
Table comments, column comments from the data dictionary, tags `layer=bronze, source_system=zillow,
ingestion=copy_into, entity=<level>, project=vstone`. Screenshot: `04_catalog_explorer_zip_ts_csv.png`

## 5. Unit tests
33 passed in GitHub Actions on PR #2 (13 new: audit columns with `assertSchemaEqual`/`assertDataFrameEqual`
via `DataFrame.transform`, COPY INTO / DDL builders, comment generation, sanity checks detecting
null keys, duplicates, missing audit values, extra columns). Screenshot: `05_ci_33_passed.png`
