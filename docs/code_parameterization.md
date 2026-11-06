# Code parameterization and triggers

Trainer review (05-11-2026): "Implement code parameterization for your pipelines and your jobs", "trigger all
pipelines and jobs through GitHub Actions, no manual triggers through UI", "do not hardcode anything anywhere".

## How a value reaches the code

```
conf/vstone_config.json  ->  databricks.yml variables  ->  job parameters   (every task of the job)
                                                       ->  task parameters  (one task: base_parameters)
                                                       ->  pipeline configuration (DLT has no widgets)
                         ->  widgets declared in the notebook's "Parameters" cell  ->  get_context()
```

* **Widgets**: every notebook starts with a *Parameters* cell that declares its widgets with
  `dbutils.widgets.text(...)`. The default value comes from `conf/vstone_config.json -> defaults` and is used only
  when the notebook is run by hand.
* **Job parameters** (`parameters:` in `resources/job_*.yml`) override the widget defaults for every task.
* **Task parameters** (`base_parameters:`) override them for one task only.
* **DLT pipeline**: values arrive through `configuration:` and are read with `spark.conf.get("vstone.<name>")`.
* `get_context()` only *reads* widgets, then builds every table name and path from them.

## Parameters per job

| Job | Job parameters (all tasks) | Task parameters |
|---|---|---|
| Data Chunking | catalog, landing_schema, bronze_schema, ops_schema, raw_volume, chunks_volume, chunk1/2/3_end_date, env, run_id | chunk_dataset: `force_rechunk` |
| vstone_bronze_ingestion | catalog, landing_schema, bronze_schema, ops_schema, raw_volume, chunks_volume, env, run_id | copy_into: `chunk_no`=1, `include_reference`; autoloader: `chunk_no`=3; xml: `chunk_no`=4; sanity checks: `chunk_nos`, `include_reference` |
| vstone_bronze_csv_dlt (pipeline) | configuration: vstone.catalog, landing_schema, raw_volume, chunks_volume, `vstone.chunk_no`=2, env, framework_path | - |
| vstone_silver_transform | catalog, landing_schema, raw_volume, bronze_schema, silver_schema, ops_schema, env, run_id | silver_time_series: `levels`; silver_features: `demo_region` |

`env` = `${bundle.target}` (dev/prod), `run_id` = `{{job.run_id}}`; every other value is a `${var.*}` bundle variable.

Example: the same Bronze notebook pattern loads any chunk. `chunk_no` picks the folder, file, table name and
audit `source` from `conf/vstone_config.json -> chunks`, and the notebook refuses a chunk configured for another
ingestion method.

## Triggers
Jobs and pipelines are never started with "Run now" in the Databricks UI:

| Event | GitHub Actions does |
|---|---|
| Pull request into dev/main | unit + integration tests on real-data samples, `bundle validate` |
| Push (merge) to dev | tests -> `bundle deploy -t dev` -> `bundle run` Data Chunking -> Bronze ingestion -> Silver transform |
| Push (merge) to main | tests -> `bundle deploy -t prod` |
| Actions -> databricks-ci-cd -> **Run workflow** | tests -> deploy to dev -> run the job picked in the form (all / one job / none) |

## Guards (tests/unit/test_no_hardcoding.py)
* No `/Volumes` paths, `catalog.schema` names, e-mail addresses, dates, raw file names, chunk file names or ops
  table names written in code - they must come from config or parameters.
* Every widget a job task's notebook declares is passed by that job (job or task parameter).
* Every job/task parameter value is a bundle variable (`${var.*}`, `${bundle.*}`, `{{job.*}}`).
* Every setting the DLT pipeline reads is in its `configuration` block.
* Every notebook ends with a "What this notebook achieved" markdown cell.
