# Day 3 Evidence - Bronze: Auto Loader (JSON), PySpark (XML), DLT (CSV) + Evaluation 1

Branch: `feature/bronze-layer` -> merged into `dev` (PR #2) | Date: 2026-11-03
Screenshots go in `docs/evidence/day3/`.

## 1. What was built
| Component | File | Method |
|---|---|---|
| JSON chunk 3 -> 6 tables `<level>_ts_json` | `src/notebooks/03b_bronze_autoloader_json.py` | Auto Loader (`cloudFiles`, `availableNow`, checkpoint, explicit schema, `_rescued_data`) |
| XML chunk 4 -> 6 tables `<level>_ts_xml` | `src/notebooks/03c_bronze_xml.py` | `spark.read.format("xml")` + `write.saveAsTable`; only new files read |
| CSV chunk 2 -> 6 streaming tables `<level>_ts_csv_dlt` | `src/pipelines/bronze_csv_dlt.py`, `resources/pipeline_bronze_csv_dlt.yml` | Delta Live Tables with expectations |
| Bronze job (4 loads in parallel -> sanity checks) | `resources/job_bronze_ingestion.yml` | DABs, serverless, retries off, email on failure |
| Central config | `conf/vstone_config.json` | All static definitions |
| Real-data fixtures + integration tests | `tests/fixtures/`, `tests/integration/` | Small slices of the actual files |
| Kaggle API ingestion | `src/notebooks/00c_kaggle_ingest.py` | Token in secret scope `vstone` |

## 2. Results
| Check | Result | Screenshot |
|---|---|---|
| Job run 1 | DLT, XML, COPY INTO green; Auto Loader loaded data, then failed on row counting (see 4) | `01_bronze_run1.png` |
| Job run 2 (after fix) | All 5 tasks green, 0 new rows in every loader (idempotent) | `02_bronze_run2.png` |
| Sanity checks | **All 148 checks passed across 26 Bronze tables** | `03_sanity_148_pass.png` |
| Values in the right columns (ZIP 01001 ZHVI_AllHomes) | csv 2000-01 = 113500, dlt 2010-01 = 186500, json 2014-01 = 177400, xml 2017-01 = 194000 - all equal the source file | `04_values_by_format.png` |
| DLT pipeline graph + expectations | 6 tables, expectation metrics | `05_dlt_graph.png` |

Row counts per method for ZIP (= chunk manifest): 1,802,455 (csv) / 1,444,165 (dlt) / 680,606 (json) / 456,659 (xml).

## 3. Issues found and fixed (engineering log)
1. **CSV schema applied by position** - real-data integration tests showed that Spark maps an explicit CSV
   schema by column position; `fields_per_level.json` lists columns in a different order from the file header,
   so DLT would have shifted values. Fix: `bronze.header_ordered_columns` takes the order from the file header
   and validates it against the contract. Regression test: `test_csv_schema_in_wrong_order_shifts_values_positional_trap`.
2. **ANSI mode** - serverless runs with ANSI on, where `to_date` raises on bad text. Switched to
   `try_to_timestamp`; test `test_chunking_is_ansi_safe`.
3. **Auto Loader row count** - `recentProgress` contained `None` for empty batches (`int + NoneType`). Data was
   already written; counting changed to table count after - before.
4. **Workspace deploy dialog timeout** - UI-side timeout; re-tried successfully.

## 4. Modularity refactor (before merge)
`runtime.get_context` replaced per-notebook widget/path code; `runner.run_per_table` replaced the copied
per-table loop; `chunk_io`, `manifest` moved logic out of notebooks; expected counts/sizes moved to config.
Regression check after refactor: Data Chunking - all files skipped, 10,584,674 rows reconciled; Bronze - 0 new
rows, 148/148 checks pass. Module reference: `docs/framework_modules.md`.

## 5. Tests and CI/CD
70 tests (unit + integration on real-data fixtures) pass in GitHub Actions.
Merge of PR #2 into `dev` ran: tests -> `bundle deploy -t dev` -> `bundle run data_chunking` ->
`bundle run bronze_ingestion` on serverless - all green. Screenshot: `06_actions_deploy_and_run.png`

## 6. Evaluation 1 checklist
| Check | Evidence |
|---|---|
| Ingestion pipelines for all three formats | CSV (COPY INTO + DLT), JSON (Auto Loader), XML (PySpark) |
| Schema validation | Header contract, explicit schemas, `_rescued_data` empty, column-order regression test |
| Error handling | Per-table runner + `ops.pipeline_run_log`, task fails at end, email on failure, no auto-retry |
| Audit columns | `load_dt`, `source`, `source_file`, `batch_id` on all 26 tables, checked |
| Data landed in Unity Catalog Bronze | `vstone.bronze` - 26 tables |
