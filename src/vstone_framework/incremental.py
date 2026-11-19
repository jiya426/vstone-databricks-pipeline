"""Day 7: incremental load of late-arriving data and MERGE INTO Silver.

Scenario (simulated, because the Zillow extract ends in Dec 2017 - see config `late_arriving`):
  * revised rows : the latest month of N ZIP codes arrives again with a revised home value -> MERGE UPDATE
  * new rows     : the next month arrives for the same ZIP codes                          -> MERGE INSERT
The batch lands as ONE CSV file in its own Volume, Auto Loader picks up only new files (incremental),
and MERGE upserts only rows whose values changed - re-running the same batch changes nothing.
"""
from __future__ import annotations

from typing import List, Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def late_batch_frame(source: DataFrame, columns: Sequence[str], level_col: str, level: str, metric_code: str,
                     region_count: int, revision_pct: float, growth_pct: float) -> DataFrame:
    """Build the late-arriving batch (all values STRING, combined column layout) from real Bronze rows.

    Picks the first `region_count` regions (by RegionName) that have `metric_code` in the latest month of `level`;
    returns their latest-month row with the metric revised by `revision_pct` %, plus a new row for the next month
    with the revised value grown by `growth_pct` % (other metrics NULL - not reported yet)."""
    rows = source.filter((F.col(level_col) == level) & F.col(metric_code).isNotNull())
    latest = rows.agg(F.max("Date").alias("Date"))
    picked = (rows.join(latest, "Date")
                  .withColumn("_rn", F.row_number().over(Window.orderBy("RegionName")))
                  .filter(F.col("_rn") <= region_count).drop("_rn"))
    revised_value = F.round(F.col(metric_code).cast("double") * (1 + revision_pct / 100), 0)
    revised = picked.withColumn(metric_code, revised_value.cast("long").cast("string"))
    next_date = F.date_format(F.last_day(F.add_months(F.to_date("Date"), 1)), "yyyy-MM-dd")
    new_month = revised.select(
        *[next_date.alias(c) if c == "Date"
          else F.round(F.col(metric_code).cast("double") * (1 + growth_pct / 100), 0).cast("long").cast("string").alias(c)
          if c == metric_code
          else F.col(c) if c in ("RegionName", level_col)
          else F.lit(None).cast("string").alias(c)
          for c in columns])
    return revised.select(*columns).unionByName(new_month)


def merge_sql(target: str, source_view: str, keys: Sequence[str], columns: Sequence[str]) -> str:
    """Idempotent upsert: update a matched row only when a value really changed, insert new keys.
    `<=>` is null-safe equality, so NULL = NULL counts as unchanged."""
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    compared = [c for c in columns if c not in keys]
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in compared)
    return (f"MERGE INTO {target} t USING {source_view} s ON {on}\n"
            f"WHEN MATCHED AND ({changed}) THEN UPDATE SET *\n"
            f"WHEN NOT MATCHED THEN INSERT *")


def value_columns(columns: Sequence[str], audit_columns: Sequence[str]) -> List[str]:
    """Columns MERGE compares: business columns only (audit columns always differ between loads)."""
    return [c for c in columns if c not in set(audit_columns)]


def merge_metrics(history_row) -> dict:
    """Rows updated / inserted by the last MERGE, from DESCRIBE HISTORY operationMetrics."""
    m = dict(history_row["operationMetrics"] or {})
    return {"operation": history_row["operation"], "version": history_row["version"],
            "updated": int(m.get("numTargetRowsUpdated", 0)), "inserted": int(m.get("numTargetRowsInserted", 0)),
            "source_rows": int(m.get("numSourceRows", 0))}
