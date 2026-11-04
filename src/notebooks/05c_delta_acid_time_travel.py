# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05c_delta_acid_time_travel - Day 5: Delta Lake ACID and time travel evidence
# MAGIC
# MAGIC Runs on a **deep clone** of `silver.state_time_series` (`ops.time_travel_demo`) so real tables are never
# MAGIC changed. Every step is asserted and recorded in `ops.time_travel_evidence` for the evidence document.
# MAGIC
# MAGIC | Part | Property shown | How |
# MAGIC |---|---|---|
# MAGIC | 1 | History on a real table | `DESCRIBE HISTORY silver.zip_time_series` - every job run is a version |
# MAGIC | 2 | Versions | clone -> UPDATE -> DELETE -> each creates a new version with operation metrics |
# MAGIC | 3 | Time travel | `VERSION AS OF` and `TIMESTAMP AS OF` read the table before the bad changes |
# MAGIC | 4 | Recovery | `RESTORE TABLE ... TO VERSION AS OF` undoes the bad changes |
# MAGIC | 5 | **A**tomicity | a batch with one invalid row (CHECK constraint) is rejected as a whole - 0 rows land |
# MAGIC | 6 | **C**onsistency | schema enforcement rejects a write with an unexpected column |
# MAGIC | 7 | **I**solation | a reader pinned to a version keeps seeing the same snapshot while the table changes |
# MAGIC | 8 | **D**urability | committed versions persist in the transaction log (`_delta_log`) and stay queryable |

# COMMAND ----------

import os, sys, time
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from datetime import datetime, timezone
from pyspark.sql import functions as F
from vstone_framework.config import load_config
from vstone_framework.runtime import get_context

cfg = load_config()
ctx = get_context(dbutils)
source = f"{ctx.silver}.state_{cfg['silver']['time_series_suffix']}"
demo = f"{ctx.ops}.time_travel_demo"
evidence = f"{ctx.ops}.time_travel_evidence"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {evidence} (run_id STRING, step INT, property STRING, action STRING,
  version BIGINT, rows BIGINT, value DOUBLE, result STRING, recorded_at TIMESTAMP)
  COMMENT 'Evidence of Delta ACID and time-travel behaviour (Day 5)'""")
log = []

def record(step, prop, action, version=None, rows=None, value=None, result="OK"):
    log.append((ctx.run_id, step, prop, action, version, rows, value, result, datetime.now(timezone.utc)))
    print(f"[{step}] {prop:12} {action:60} version={version} rows={rows} value={value} -> {result}")

def latest_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version

def ca_value(table_or_df, version=None):
    q = spark.read.option("versionAsOf", version).table(table_or_df) if version is not None else spark.table(table_or_df)
    return q.filter("region_name = 'California' AND date = DATE'2017-12-31'").first().zhvi_all_homes

# COMMAND ----------

# MAGIC %md ## 1. History on a real Silver table (each job run = one version)

# COMMAND ----------

hist = spark.sql(f"DESCRIBE HISTORY {ctx.silver}.zip_{cfg['silver']['time_series_suffix']}")
display(hist.select("version", "timestamp", "operation", "operationParameters", "operationMetrics.numOutputRows"))
record(1, "history", f"DESCRIBE HISTORY silver.zip_time_series ({hist.count()} versions)", rows=hist.count())

# COMMAND ----------

# MAGIC %md ## 2. Create versions: clone, bad UPDATE, accidental DELETE

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {demo} DEEP CLONE {source}")
v_clone = latest_version(demo)
rows0, value0 = spark.table(demo).count(), ca_value(demo)
record(2, "versions", "DEEP CLONE silver.state_time_series", v_clone, rows0, value0)
ts_before_changes = spark.sql(f"DESCRIBE HISTORY {demo} LIMIT 1").first().timestamp
time.sleep(2)  # make TIMESTAMP AS OF unambiguous

spark.sql(f"UPDATE {demo} SET zhvi_all_homes = zhvi_all_homes * 10 WHERE region_name = 'California'")
v_update = latest_version(demo)
record(2, "versions", "UPDATE: California values wrongly x10", v_update, spark.table(demo).count(), ca_value(demo))

spark.sql(f"DELETE FROM {demo} WHERE region_name = 'Texas'")
v_delete = latest_version(demo)
rows_after_delete = spark.table(demo).count()
record(2, "versions", "DELETE: Texas rows removed by mistake", v_delete, rows_after_delete, ca_value(demo))
display(spark.sql(f"DESCRIBE HISTORY {demo}").select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md ## 3. Time travel: read the table as it was before the mistakes

# COMMAND ----------

by_version = spark.sql(f"SELECT COUNT(*) n FROM {demo} VERSION AS OF {v_clone}").first().n
by_time = spark.sql(f"SELECT COUNT(*) n FROM {demo} TIMESTAMP AS OF '{ts_before_changes}'").first().n
assert by_version == rows0 and by_time == rows0 and ca_value(demo, v_clone) == value0
record(3, "time travel", f"VERSION AS OF {v_clone}", v_clone, by_version, ca_value(demo, v_clone))
record(3, "time travel", f"TIMESTAMP AS OF '{ts_before_changes}'", v_clone, by_time)
display(spark.sql(f"""SELECT 'current' AS state, COUNT(*) rows FROM {demo}
                      UNION ALL SELECT 'version {v_clone}', COUNT(*) FROM {demo} VERSION AS OF {v_clone}"""))

# COMMAND ----------

# MAGIC %md ## 4. Recovery with RESTORE

# COMMAND ----------

spark.sql(f"RESTORE TABLE {demo} TO VERSION AS OF {v_clone}")
assert spark.table(demo).count() == rows0 and ca_value(demo) == value0
record(4, "restore", f"RESTORE TABLE TO VERSION AS OF {v_clone}", latest_version(demo), spark.table(demo).count(), ca_value(demo))

# COMMAND ----------

# MAGIC %md ## 5. Atomicity: one bad row rejects the whole batch

# COMMAND ----------

spark.sql(f"ALTER TABLE {demo} ADD CONSTRAINT zhvi_not_negative CHECK (zhvi_all_homes IS NULL OR zhvi_all_homes >= 0)")
batch = spark.table(demo).filter("region_name = 'Alabama'").limit(5)
first_date = batch.agg(F.min("date")).first()[0]
bad_batch = batch.withColumn("zhvi_all_homes", F.when(F.col("date") == first_date, F.lit(-1.0)).otherwise(F.col("zhvi_all_homes")))
before = spark.table(demo).count()
try:
    bad_batch.write.mode("append").saveAsTable(demo)
    result = "UNEXPECTED: write succeeded"
except Exception as e:
    result = f"rejected: {type(e).__name__}"
after = spark.table(demo).count()
assert after == before, "partial write detected"
record(5, "atomicity", "append 5 rows, 1 violates CHECK zhvi >= 0", latest_version(demo), after - before, None, result)

# COMMAND ----------

# MAGIC %md ## 6. Consistency: schema enforcement

# COMMAND ----------

wrong_schema = spark.table(demo).limit(1).withColumn("unexpected_column", F.lit("x"))
try:
    wrong_schema.write.mode("append").saveAsTable(demo)
    result = "UNEXPECTED: write succeeded"
except Exception as e:
    result = f"rejected: {type(e).__name__}"
assert spark.table(demo).count() == before and "unexpected_column" not in spark.table(demo).columns
record(6, "consistency", "append with an extra column (no mergeSchema)", latest_version(demo), 0, None, result)

# COMMAND ----------

# MAGIC %md ## 7. Isolation: a reader pinned to a version sees a stable snapshot

# COMMAND ----------

pinned_version = latest_version(demo)
pinned = spark.read.option("versionAsOf", pinned_version).table(demo)
count_before = pinned.count()
spark.sql(f"DELETE FROM {demo} WHERE region_name = 'Alabama'")        # concurrent change by a "writer"
count_after_change = pinned.count()
assert count_before == count_after_change and spark.table(demo).count() < count_before
record(7, "isolation", f"reader pinned to version {pinned_version} while rows are deleted", pinned_version,
       count_after_change, None, f"snapshot unchanged ({count_before}); current table {spark.table(demo).count()}")

# COMMAND ----------

# MAGIC %md ## 8. Durability: committed versions persist and remain queryable

# COMMAND ----------

versions = spark.sql(f"DESCRIBE HISTORY {demo}").count()
oldest = spark.sql(f"SELECT COUNT(*) n FROM {demo} VERSION AS OF {v_clone}").first().n
assert oldest == rows0
record(8, "durability", f"{versions} committed versions in _delta_log; version {v_clone} still readable", v_clone, oldest)

spark.createDataFrame(log, "run_id STRING, step INT, property STRING, action STRING, version BIGINT, rows BIGINT, "
                           "value DOUBLE, result STRING, recorded_at TIMESTAMP").write.mode("append").saveAsTable(evidence)
display(spark.table(evidence).filter(F.col("run_id") == ctx.run_id).orderBy("step", "recorded_at"))
print("All ACID and time-travel checks passed.")
