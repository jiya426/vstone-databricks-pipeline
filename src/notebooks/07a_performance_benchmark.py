# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 07a_performance_benchmark - Liquid Clustering vs partitioning + Z-order (PDF Day 7)
# MAGIC
# MAGIC **Goal:** measure which data layout serves our real query patterns best. The Gold fact
# MAGIC (region x metric x month) is copied into 3 tables that differ **only** in layout:
# MAGIC
# MAGIC | Layout | DDL | Maintenance |
# MAGIC |---|---|---|
# MAGIC | `plain` | none | - |
# MAGIC | `partition_zorder` | `PARTITIONED BY (metric_key)` | `OPTIMIZE ... ZORDER BY (region_key, date_key)` |
# MAGIC | `liquid` | `CLUSTER BY (metric_key, region_key, date_key)` | `OPTIMIZE` (incremental clustering) |
# MAGIC
# MAGIC The same 5 queries (point lookup, one month, a date range, a prefix filter, a full scan - config
# MAGIC `benchmark.queries`) run `benchmark_runs` times on each table. For each query we record the median time
# MAGIC and **how many files hold matching rows** (what data skipping can prune to). Results go to
# MAGIC `ops.performance_benchmark`; the write-up is `docs/performance_benchmark.md`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Job parameters + task parameters `benchmark_runs` and `benchmark_layouts` arrive as widgets.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.runtime import get_context, widget_defaults

d = widget_defaults()
# Job parameters
dbutils.widgets.text("catalog", d["catalog"], "Catalog")
dbutils.widgets.text("gold_schema", d["gold_schema"], "Gold schema")
dbutils.widgets.text("ops_schema", d["ops_schema"], "Ops schema")
dbutils.widgets.text("env", d["env"], "Environment")
dbutils.widgets.text("run_id", d["run_id"], "Job run id")
# Task parameters
dbutils.widgets.text("benchmark_runs", d["benchmark_runs"], "Runs per query")
dbutils.widgets.text("benchmark_layouts", d["benchmark_layouts"], "Layouts")

ctx = get_context(dbutils, ["benchmark_runs", "benchmark_layouts"])

# COMMAND ----------

import statistics
import time
from datetime import datetime, timezone
from vstone_framework.config import load_config

cfg = load_config()
b = cfg["benchmark"]
source = f"{ctx.gold}.{cfg['gold']['tables']['fact']}"
layouts = ctx.list_param("benchmark_layouts", list(b["layouts"]))
runs = ctx.int_param("benchmark_runs")
results_table = f"{ctx.ops}.{b['results_table']}"
bench = lambda layout: f"{ctx.ops}.{b['layouts'][layout]['table']}"

# COMMAND ----------

# MAGIC %md ## 1. Build the 3 copies (same rows, different layout)

# COMMAND ----------

build = []
for layout in layouts:
    spec, table = b["layouts"][layout], bench(layout)
    t0 = time.time()
    spark.sql(f"CREATE OR REPLACE TABLE {table} {spec['ddl']} "
              f"COMMENT 'Benchmark copy of {source} - layout {layout}' AS SELECT * FROM {source}")
    spark.sql(f"OPTIMIZE {table} {spec['optimize']}")
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
    build.append((layout, table, round(time.time() - t0, 1), detail.numFiles, detail.sizeInBytes,
                  ", ".join(detail.partitionColumns or []), ", ".join(detail.clusteringColumns or [])))
display(spark.createDataFrame(build, "layout STRING, table_name STRING, build_seconds DOUBLE, files LONG, "
                                     "size_bytes LONG, partition_columns STRING, clustering_columns STRING"))

# COMMAND ----------

# MAGIC %md ## 2. Run the same queries on every layout

# COMMAND ----------

def files_with_matches(table, sql):
    """Files that contain at least one row the query's filter selects (= the minimum a reader must open)."""
    where = sql.split(" WHERE ", 1)[1].split(" GROUP BY ")[0] if " WHERE " in sql else "TRUE"
    return spark.sql(f"SELECT COUNT(DISTINCT _metadata.file_path) FROM {table} WHERE {where}").first()[0]

rows = []
measured_at = datetime.now(timezone.utc)
for layout in layouts:
    table = bench(layout)
    total_files = spark.sql(f"DESCRIBE DETAIL {table}").first().numFiles
    for q in b["queries"]:
        sql = q["sql"].format(table=table)
        timings = []
        for _ in range(runs):
            t0 = time.time()
            n = len(spark.sql(sql).collect())
            timings.append(time.time() - t0)
        rows.append((ctx.run_id, layout, q["id"], q["description"], runs, round(timings[0], 3),
                     round(statistics.median(timings), 3), n, files_with_matches(table, sql), total_files,
                     measured_at))
        print(f"{layout:16s} {q['id']}  median {statistics.median(timings):6.2f}s  first {timings[0]:6.2f}s")

schema = ("run_id STRING, layout STRING, query_id STRING, description STRING, runs INT, first_run_seconds DOUBLE, "
          "median_seconds DOUBLE, rows_returned LONG, files_with_matches LONG, total_files LONG, measured_at TIMESTAMP")
spark.sql(f"CREATE TABLE IF NOT EXISTS {results_table} ({schema}) "
          f"COMMENT 'Day 7 layout benchmark: query time and data skipping per layout'")
spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(results_table)

# COMMAND ----------

# MAGIC %md ## 3. Comparison (median seconds and files with matching rows)

# COMMAND ----------

pivot = ",\n".join([f"MAX(CASE WHEN layout = '{l}' THEN median_seconds END) AS {l}_s" for l in layouts] +
                   [f"MAX(CASE WHEN layout = '{l}' THEN files_with_matches END) AS {l}_files" for l in layouts])
display(spark.sql(f"""SELECT query_id, description, {pivot} FROM {results_table}
                      WHERE run_id = '{ctx.run_id}' GROUP BY query_id, description ORDER BY query_id"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook achieved
# MAGIC - Built 3 copies of the Gold fact that differ only in layout: none, partitioning + Z-order, Liquid Clustering.
# MAGIC - Timed the same 5 real query patterns on each and counted the files that hold matching rows (data skipping).
# MAGIC - Stored every measurement in `ops.performance_benchmark` for the benchmark report.
