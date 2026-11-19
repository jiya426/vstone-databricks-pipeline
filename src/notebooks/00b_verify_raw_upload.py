# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00b_verify_raw_upload
# MAGIC Checks that all 11 archive files are in the raw Volume with the exact byte sizes recorded in
# MAGIC `conf/vstone_config.json` (expected_raw_bytes), so a truncated upload is caught immediately.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Job parameters arrive as widgets (values from `databricks.yml` variables); the defaults come from
# MAGIC `conf/vstone_config.json` and are used only when the notebook is run by hand.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("landing_schema", d["landing_schema"], "Landing schema")
dbutils.widgets.text("raw_volume", d["raw_volume"], "Raw files Volume")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")

ctx = get_context(dbutils)

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.config import load_config
from vstone_framework.runtime import get_context

expected_bytes = load_config()["expected_raw_bytes"]

# COMMAND ----------

actual = {f.name: f.size for f in dbutils.fs.ls(ctx.raw_path)}
rows = [(name, size, actual.get(name),
         "OK" if actual.get(name) == size else ("MISSING" if name not in actual else "SIZE MISMATCH"))
        for name, size in expected_bytes.items()]
display(spark.createDataFrame(rows, "file STRING, expected_bytes LONG, actual_bytes LONG, status STRING"))
problems = [r[0] for r in rows if r[3] != "OK"]
assert not problems, f"Re-upload these files: {problems}"
print(f"All {len(rows)} files present with correct sizes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Checked that all 11 raw files are in the raw Volume with exactly the expected byte sizes (from config).
# MAGIC - Proves the upload is complete and not truncated before anything else runs.
