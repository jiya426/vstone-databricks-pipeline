# Vstone: housing market lakehouse on Databricks

An end-to-end Medallion lakehouse on Databricks Free Edition, built on the Zillow Economics dataset (Kaggle `zillow/zecon`).
Raw files land in a Volume, flow through Bronze, Silver and Gold, and are served to dashboards and Genie with
row-level security and column masks by access level.

## Architecture

| Layer | Schema | What it holds |
|---|---|---|
| Landing | Volumes in `vstone.landing` | Raw Zillow files, the 4 chunk files, the late-arriving batch, checkpoints |
| Bronze | `vstone.bronze` | Raw data as Delta. All columns are strings, plus audit columns |
| Silver | `vstone.silver` | Typed, cleaned, deduplicated and standardised data; quarantine for bad rows |
| Gold | `vstone.gold` | Star schema (dimensions, facts) and aggregates. Secured views for consumers |
| Governance | `vstone.governance` | Access-level function, row filter, column masks, access map, policy catalog |
| Ops | `vstone.ops` | Run log, sanity-check results, chunk manifest, profiling, benchmark results |

**Data chunking.** The six time-series files are combined into one dataset with a `RegionLevel` column, then split by
date into exactly four files: two CSV, one JSON and one XML. Each file has its own Bronze table.

**Gold data model.** Star schema: `dim_date` (static), `dim_region` (SCD Type 2), `dim_metric` (SCD Type 1) and
`fact_housing_metric_monthly`. Aggregates are built on top of the fact. The design rationale and the planned changes
are in `docs/data_model_rationale.md`, and the editable diagram is linked from there.

**Security.** One access-level function decides each user's level: admin, analyst, viewer or none. Row filters and
column masks use that level, so the same query returns different rows and values per role. Details are in
`docs/security_rls_cls.md`.

## Jobs and pipelines

| Name | Type | What it does |
|---|---|---|
| `master_pipeline` | Job | Preflight, then chunking, Bronze, Silver, Gold and security, in order |
| `job_data_chunking` | Job | Clears the old layout, creates the 4 chunk files, validates them |
| `job_bronze_ingestion` | Job | Loads the 4 chunk files and the crosswalks into Bronze |
| `pipeline_bronze_csv_dlt` | Lakeflow pipeline | Declarative Bronze table for the CSV chunk |
| `job_silver_transform` | Job | Typing, cleaning, quarantine, conformed tables and features |
| `gold_dlt` | Lakeflow pipeline | Dimensions, fact and aggregates |
| `job_gold` | Job | Runs `gold_dlt`, then the Gold checks and the glossary |
| `job_security` | Job | Access-level function, masks, row filter, secured views, grants |
| `job_incremental_load` | Job | Day 7: late-arriving data through Auto Loader and MERGE |
| `job_performance_benchmark` | Job | Day 7: plain vs partition vs Liquid layouts |

## How to run

Everything runs from GitHub Actions. Do not start jobs from the Databricks UI.

- **Push to `dev`:** runs the tests, deploys to dev, and runs the master pipeline.
- **Pull request into `dev` or `main`:** runs the tests and validates the bundle. This is the gate.
- **Run workflow (manual):** runs the tests, deploys to dev, then runs the job you pick. Options include `gold`,
  `security`, `performance_benchmark`, and `refresh_dim_metric` (a one-off full refresh of `dim_metric`).
- **Push to `main`:** runs the tests and deploys to prod.

## Configuration and parameters

- `conf/vstone_config.json` holds every static value: table and file names, chunk definitions, Gold settings,
  security groups and mask rules.
- `databricks.yml` holds the bundle variables for each environment (catalog, schemas, email, dates).
- Jobs pass task parameters, and each notebook reads them through widgets. Defaults come from the config.
- No table, path or schema name is hardcoded in code. A unit test scans the code to check this.

## Tests

```
pip install -r requirements-dev.txt
pytest tests -v
```

- `tests/unit`: the chunking, bronze, gold, security and orchestration logic, plus the hardcoding check.
- `tests/integration`: the chunking, Bronze and Gold steps on a real sample of the data.

The suite currently runs 264 tests. Each Gold and security change is also verified in Databricks by its job.

## Documentation

| File | Contents |
|---|---|
| `docs/requirements_and_assumptions.md` | Requirements, trainer points and assumptions |
| `docs/data_model_rationale.md` | Star schema decisions, SCD types, keys, CDC, planned changes |
| `docs/gold_data_model.md` | Current Gold tables, keys and ER diagram |
| `docs/business_glossary.md` | Business terms used across the layers |
| `docs/genie_space.md` | Genie benchmarks and the 80% acceptance threshold |
| `docs/incremental_load_and_scd2.md` | Day 7: late data, MERGE and SCD2 versions |
| `docs/performance_benchmark.md` | Layout benchmark results |
| `docs/security_rls_cls.md` | Access levels, masks, row filter and demo script |
| `docs/dbu_dashboard.md` | Cost and usage dashboard queries |
| `docs/dashboard_housing_market.md` | Housing market dashboard and its datasets |
| `docs/code_parameterization.md` | How parameters flow from config to notebooks |
| `docs/framework_modules.md` | The Python framework modules |
