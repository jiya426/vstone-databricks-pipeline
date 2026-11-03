# Databricks notebook source
# MAGIC %md
# MAGIC # 00b_verify_raw_upload
# MAGIC Checks that all 11 archive files are in the raw Volume with the exact byte sizes
# MAGIC of the original archive, so a truncated upload is caught immediately.

# COMMAND ----------

dbutils.widgets.text("catalog", "vstone")
dbutils.widgets.text("landing_schema", "landing")
dbutils.widgets.text("raw_volume", "raw")
raw_path = (f"/Volumes/{dbutils.widgets.get('catalog')}/"
            f"{dbutils.widgets.get('landing_schema')}/{dbutils.widgets.get('raw_volume')}/")

expected_bytes = {
    "City_time_series.csv": 689627012,
    "Zip_time_series.csv": 782137407,
    "Neighborhood_time_series.csv": 265790699,
    "County_time_series.csv": 112778579,
    "Metro_time_series.csv": 56232114,
    "State_time_series.csv": 4742679,
    "CountyCrossWalk_Zillow.csv": 232467,
    "cities_crosswalk.csv": 1074309,
    "DataDictionary.csv": 4668,
    "all_available_metrics.json": 3409,
    "fields_per_level.json": 17005,
}

# COMMAND ----------

actual = {f.name: f.size for f in dbutils.fs.ls(raw_path)}
rows, problems = [], []
for name, size in expected_bytes.items():
    got = actual.get(name)
    status = "OK" if got == size else ("MISSING" if got is None else "SIZE MISMATCH")
    rows.append((name, size, got, status))
    if status != "OK":
        problems.append(name)

display(spark.createDataFrame(rows, "file STRING, expected_bytes LONG, actual_bytes LONG, status STRING"))
assert not problems, f"Re-upload these files: {problems}"
print("All 11 files present with correct sizes.")
