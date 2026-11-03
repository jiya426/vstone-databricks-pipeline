# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00c_kaggle_ingest - automated ingestion from Kaggle (trainer requirement)
# MAGIC
# MAGIC **What:** downloads the Zillow Economics dataset (`zillow/zecon`) with the Kaggle API straight into the
# MAGIC Unity Catalog Volume `landing.raw_kaggle`, then verifies every file's size against the expected sizes in
# MAGIC `conf/vstone_config.json`.
# MAGIC
# MAGIC **Security:** the Kaggle username and API key are read from the Databricks **secret scope** `vstone`
# MAGIC (`dbutils.secrets.get`) - never typed in code. Secret values are redacted in notebook output.
# MAGIC
# MAGIC **Why a separate Volume:** the pipeline reads from `landing.raw` (UI upload, already verified).
# MAGIC This notebook proves automated, repeatable ingestion without touching the data the pipeline uses.
# MAGIC
# MAGIC **Free Edition note:** outbound internet is limited to trusted domains unless the account is
# MAGIC LinkedIn-verified. If kaggle.com is blocked, the notebook stops with a clear message and the
# MAGIC verified UI upload remains the source.
# MAGIC
# MAGIC One-time secret setup (Databricks CLI on your laptop):
# MAGIC ```
# MAGIC databricks secrets create-scope vstone
# MAGIC databricks secrets put-secret vstone kaggle_username
# MAGIC databricks secrets put-secret vstone kaggle_key
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install kaggle --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, shutil, sys, tempfile, zipfile
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.config import load_config

cfg = load_config()
kcfg = cfg["kaggle"]
dbutils.widgets.text("catalog", "vstone")
dbutils.widgets.text("landing_schema", "landing")
catalog, landing = dbutils.widgets.get("catalog"), dbutils.widgets.get("landing_schema")
target = f"/Volumes/{catalog}/{landing}/{kcfg['target_volume']}"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{landing}.{kcfg['target_volume']} "
          "COMMENT 'Zillow files downloaded automatically with the Kaggle API'")

# COMMAND ----------

# 1. Credentials from the secret scope (never hard-coded)
try:
    os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get(kcfg["secret_scope"], kcfg["username_key"])
    os.environ["KAGGLE_KEY"] = dbutils.secrets.get(kcfg["secret_scope"], kcfg["token_key"])
except Exception as e:
    raise RuntimeError(f"Secret scope '{kcfg['secret_scope']}' or its keys are missing. "
                       "Create them with the Databricks CLI commands at the top of this notebook.") from e
print("Credentials loaded from secret scope:", kcfg["secret_scope"])

# COMMAND ----------

# 2. Download the dataset zip (network access may be blocked on Free Edition)
from kaggle.api.kaggle_api_extended import KaggleApi

tmp = tempfile.mkdtemp()
try:
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(kcfg["dataset"], path=tmp, unzip=False, quiet=False)
except Exception as e:
    msg = (f"Kaggle download failed: {type(e).__name__}: {str(e)[:300]}\n"
           "Most likely cause on Free Edition: outbound internet to kaggle.com is blocked "
           "(available after LinkedIn verification). The pipeline keeps using the verified UI upload in landing.raw.")
    print(msg)
    dbutils.notebook.exit(msg)

zip_path = [os.path.join(tmp, f) for f in os.listdir(tmp) if f.endswith(".zip")][0]
print("Downloaded", zip_path, os.path.getsize(zip_path), "bytes")

# COMMAND ----------

# 3. Extract each file straight into the Volume (streamed, no second local copy)
with zipfile.ZipFile(zip_path) as zf:
    for member in zf.infolist():
        if member.is_dir():
            continue
        name = os.path.basename(member.filename)
        with zf.open(member) as src, open(f"{target}/{name}", "wb") as dst:
            shutil.copyfileobj(src, dst, length=16 * 1024 * 1024)
        print("extracted", name)
shutil.rmtree(tmp, ignore_errors=True)

# COMMAND ----------

# 4. Verify: every expected file present with the exact byte size
actual = {f.name: f.size for f in dbutils.fs.ls(target)}
rows = [(n, s, actual.get(n), "OK" if actual.get(n) == s else "MISMATCH")
        for n, s in cfg["expected_raw_bytes"].items()]
display(spark.createDataFrame(rows, "file STRING, expected_bytes LONG, actual_bytes LONG, status STRING"))
bad = [r[0] for r in rows if r[3] != "OK"]
assert not bad, f"Files missing or different: {bad}"
print(f"Kaggle ingestion verified: {len(rows)} files in {target}")
