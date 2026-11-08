# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08a_preflight_checks - first task of the master pipeline (PDF Day 8-9)
# MAGIC
# MAGIC **Goal:** stop the whole pipeline early, with a clear message, when the platform is not ready - instead
# MAGIC of failing half-way through a long run.
# MAGIC
# MAGIC | Check | Rule |
# MAGIC |---|---|
# MAGIC | config | `conf/vstone_config.json` passes validation |
# MAGIC | catalog and schemas | the catalog and all 6 schemas exist |
# MAGIC | raw files | the raw Volume holds the 11 source files |
# MAGIC
# MAGIC **Failure-alert test:** job parameter `simulate_failure = true` makes this task fail on purpose, so the
# MAGIC master job's `email_notifications.on_failure` sends the alert e-mail (PDF: "email notification on failure").
# MAGIC Triggered from GitHub Actions -> Run workflow -> `failure_alert_test`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
# Job parameters
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("landing_schema", d["landing_schema"], "Landing schema")
dbutils.widgets.text("bronze_schema", d["bronze_schema"], "Bronze schema")
dbutils.widgets.text("silver_schema", d["silver_schema"], "Silver schema")
dbutils.widgets.text("gold_schema", d["gold_schema"], "Gold schema")
dbutils.widgets.text("governance_schema", d["governance_schema"], "Governance schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("raw_volume", d["raw_volume"], "Raw files Volume")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")
dbutils.widgets.dropdown("simulate_failure", d["simulate_failure"], ["false", "true"], "Simulate a failure")

ctx = get_context(dbutils, ["simulate_failure"])

# COMMAND ----------

from vstone_framework.config import load_config, validate_config

cfg = load_config()
checks = []

validate_config(cfg)
checks.append(("config", "valid", "PASS"))

# Schema NAMES come from the job parameters (config -> "schemas" maps each layer to its description/comment)
existing = {r[0] for r in spark.sql(f"SHOW SCHEMAS IN {ctx.catalog}").collect()}
for s in (ctx.landing_schema, ctx.bronze_schema, ctx.silver_schema, ctx.gold_schema, ctx.governance_schema,
          ctx.ops_schema):
    checks.append((f"schema {ctx.catalog}.{s}", "exists", "PASS" if s in existing else "FAIL"))

raw_files = [f.name for f in dbutils.fs.ls(ctx.raw_path) if not f.name.endswith("/")]
expected = int(cfg["orchestration"]["expected_raw_files"])
checks.append(("raw files", f"{len(raw_files)} of {expected}", "PASS" if len(raw_files) >= expected else "FAIL"))

display(spark.createDataFrame(checks, "check STRING, result STRING, status STRING"))
failed_checks = [c for c in checks if c[2] == "FAIL"]
if failed_checks:
    raise RuntimeError(f"Preflight failed: {failed_checks}")

if ctx.bool_param("simulate_failure"):
    raise RuntimeError("Simulated failure (simulate_failure = true): testing the on-failure e-mail alert. "
                       "Nothing else in the pipeline runs.")
print(f"Preflight passed ({len(checks)} checks) - env={ctx.env}, run_id={ctx.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Validated the config, the catalog and schemas, and the raw source files before any heavy work starts.
# MAGIC - With `simulate_failure = true`, failed on purpose so the job's failure e-mail can be shown.
