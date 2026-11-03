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

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.config import load_config
from vstone_framework.runtime import get_context

expected_bytes = load_config()["expected_raw_bytes"]
ctx = get_context(dbutils)

# COMMAND ----------

actual = {f.name: f.size for f in dbutils.fs.ls(ctx.raw_path)}
rows = [(name, size, actual.get(name),
         "OK" if actual.get(name) == size else ("MISSING" if name not in actual else "SIZE MISMATCH"))
        for name, size in expected_bytes.items()]
display(spark.createDataFrame(rows, "file STRING, expected_bytes LONG, actual_bytes LONG, status STRING"))
problems = [r[0] for r in rows if r[3] != "OK"]
assert not problems, f"Re-upload these files: {problems}"
print(f"All {len(rows)} files present with correct sizes.")
