# Day 1 Evidence - Setup, Profiling, Data Chunking

Branch: `feature/data-profiling` -> `dev` | Date: 2026-11-03
Screenshots go in `docs/evidence/day1/` (file names listed in each section).

## 1. Environment and Git (R01, R02, R05, R06)
| Item | Result | Screenshot |
|---|---|---|
| Workspace | Databricks **Free Edition**, serverless only | `01_workspace_free_edition.png` |
| Repo | PRIVATE `jiya426/vstone-databricks-pipeline`, DABs folder layout | `02_repo_structure.png` |
| Branches | `main`, `dev`, `feature/data-profiling` | `03_branches.png` |
| Branch protection | Rule on `main` created; shows "Not enforced" (GitHub free plan, private repo). Policy followed by convention: all changes via PR | `04_branch_rule_not_enforced.png` |
| Databricks Git folder | Repo linked via Settings > Linked accounts; branch switched and pulled in workspace (no VS Code) | `05_git_folder.png` |

## 2. CI/CD (R03, R04)
| Item | Result | Screenshot |
|---|---|---|
| `databricks.yml` | Variables for all names and cut-offs; `dev` and `prod` targets | - |
| GitHub Actions | `databricks-ci-cd` on PR #1: unit tests **20 passed**, `bundle validate -t dev` OK | `06_actions_green.png` |
| Issue found and fixed | Runner's default Java broke PySpark 3.5 (`getCurrentUserName`); fixed with `actions/setup-java` Java 17 | `07_actions_java_error.png` |

## 3. Unity Catalog setup (`00_setup`)
Output: `OK: catalog vstone, schemas ['bronze', 'gold', 'governance', 'landing', 'ops', 'silver'], volumes ['chunks', 'raw']`

## 4. Raw data landed in Volume (R09) - `00b_verify_raw_upload`
All 11 files present in `/Volumes/vstone/landing/raw/` with byte sizes identical to the archive
(e.g. Zip_time_series.csv 782,137,407 bytes). Output: `All 11 files present with correct sizes.`
Screenshot: `08_raw_upload_verified.png`

## 5. Dataset profiling (R08) - `01_data_profiling`
Final assertion passed: `Profiling validated: 10,584,674 rows across 6 files.`
Tables: `vstone.ops.profile_file_summary`, `profile_column_stats`, `profile_dq_findings`.
Key findings: 0 duplicate (Date, RegionName) keys, 0 bad dates, headers match `fields_per_level.json`,
302 malformed Neighborhood RegionNames, County join 2,443 -> 2,701 after lpad.
Screenshots: `09_profile_file_summary.png`, `10_profile_dq_findings.png`

## 6. Data Chunking job (R10, R11)
Job `[dev jjain1374] Data Chunking` (deployed with DABs), tasks `chunk_dataset` -> `validate_chunks`.

| Run | Result | Duration | Screenshot |
|---|---|---|---|
| 1 (first run) | SUCCESS, 24 chunks + 2 crosswalks written; validation passed | 9m 9s | `11_job_run1.png` |
| 2 (re-run) | SUCCESS, every chunk **skipped** (no files rewritten) | - | `12_job_run2_skipped.png` |

Manifest (`vstone.ops.chunk_manifest`, 26 rows, all SUCCESS), sample:

| Level | Chunk 1 CSV | Chunk 2 CSV | Chunk 3 JSON | Chunk 4 XML | Total = source |
|---|---|---|---|---|---|
| zip | 1,802,455 | 1,444,165 | 680,606 | 456,659 | 4,383,885 |
| neighborhood | 700,701 | 554,982 | 262,023 | 177,332 | 1,695,038 |
| state | 5,773 | 4,320 | 1,872 | 1,247 | 13,212 |

Chunk windows start 1996-04-30, 2006-01-31, 2013-01-31, 2016-01-31. Screenshot: `13_chunk_manifest.png`

Multiple-run scenarios covered (code + unit tests `test_plan_action_multiple_run_scenarios`):
first run, unchanged re-run (skip), resume after failure, changed cut-offs (blocked unless forced),
forced re-split, output deleted by hand.

## 7. Unit tests
`pytest`: 20 passed (chunk boundaries with `assertDataFrameEqual`, output schema with
`assertSchemaEqual`, transformations via `DataFrame.transform`, cut-off validation, reconciliation,
XML-name check, re-run decision matrix, header checks). Report: CI artifact `test-results`.
