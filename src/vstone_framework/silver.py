"""Silver layer: Bronze -> clean, typed, deduplicated tables (PDF Day 4).

Pipeline per geography level (each step is a DataFrame.transform-able function, unit-tested):

    union_bronze_sources   4 Bronze tables (COPY INTO, DLT, Auto Loader, XML) -> one DataFrame
    flag_quality_issues    adds `_reason` = first failing rule, else NULL
    split_quarantine       rows with a reason -> quarantine; the rest continue
    cast_types             Date STRING -> DATE, metrics STRING -> DOUBLE
    drop_empty_rows        rows where every metric is NULL carry no information
    dedupe_latest          one row per (Date, RegionName); the latest Bronze load wins
    standardize_headers    snake_case column names (Python / pandas UDF, see headers.py)

NULL policy ("clean nulls"):
  * NULL key (Date or RegionName)  -> quarantined (row is unusable)
  * every metric NULL              -> dropped and counted (no information)
  * some metrics NULL              -> kept: NULL means "not reported for this region/month"
                                       (the source is 60-85% sparse by design); Gold drops them when unpivoting
"""
from __future__ import annotations

from functools import reduce
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .audit import AUDIT_COLUMN_COMMENTS, AUDIT_COLUMNS
from .metadata import sql_string

REASON_COL = "_reason"
BRONZE_LOAD_COL = "bronze_load_dt"

# Quarantine reason codes, checked in this order (first match wins)
REASONS = {
    "NULL_KEY": "Date or RegionName is NULL",
    "BAD_DATE": "Date is not a valid yyyy-MM-dd date",
    "INVALID_REGION": "RegionName does not match the format for this level",
    "RESCUED_DATA": "Values outside the expected schema were captured in _rescued_data",
    "NON_NUMERIC": "A metric value is not a number",
}


def union_bronze_sources(dfs: Sequence[DataFrame], contract: Sequence[str], rescued_col: str) -> DataFrame:
    """Stack the Bronze tables of one level; keeps lineage (`source`, `source_file`, Bronze load time)."""
    parts = []
    for df in dfs:
        rescued = F.col(rescued_col) if rescued_col in df.columns else F.lit(None).cast("string")
        parts.append(df.select(*[F.col(f"`{c}`") for c in contract], rescued.alias(rescued_col),
                               F.col("source"), F.col("source_file"), F.col("load_dt").alias(BRONZE_LOAD_COL)))
    return reduce(lambda a, b: a.unionByName(b), parts)


def _non_numeric(metric_cols: Sequence[str]):
    checks = [F.col(f"`{c}`").isNotNull() & F.expr(f"try_cast(`{c}` AS DOUBLE)").isNull() for c in metric_cols]
    return reduce(lambda a, b: a | b, checks) if checks else F.lit(False)


def flag_quality_issues(metric_cols: Sequence[str], region_pattern: str, rescued_col: str,
                        date_col: str = "Date", region_col: str = "RegionName"):
    """Adds `_reason`: the first quarantine rule a row breaks (NULL when the row is valid)."""
    def _transform(df: DataFrame) -> DataFrame:
        date, region = F.col(date_col), F.col(region_col)
        reason = (F.when(date.isNull() | region.isNull(), F.lit("NULL_KEY"))
                  .when(F.try_to_timestamp(date, F.lit("yyyy-MM-dd")).isNull(), F.lit("BAD_DATE"))
                  .when(~region.rlike(region_pattern), F.lit("INVALID_REGION"))
                  .when(F.col(rescued_col).isNotNull(), F.lit("RESCUED_DATA"))
                  .when(_non_numeric(metric_cols), F.lit("NON_NUMERIC")))
        return df.withColumn(REASON_COL, reason)
    return _transform


def split_quarantine(df: DataFrame):
    """Returns (valid rows without `_reason`, quarantined rows with `_reason`)."""
    return df.filter(F.col(REASON_COL).isNull()).drop(REASON_COL), df.filter(F.col(REASON_COL).isNotNull())


def cast_types(metric_cols: Sequence[str], date_col: str = "Date"):
    """Date -> DATE, metrics -> DOUBLE (try_cast: safe with ANSI mode; values were validated before)."""
    def _transform(df: DataFrame) -> DataFrame:
        out = df.withColumn(date_col, F.try_to_timestamp(F.col(date_col), F.lit("yyyy-MM-dd")).cast("date"))
        return out.select(*[F.expr(f"try_cast(`{c}` AS DOUBLE)").alias(c) if c in metric_cols else F.col(f"`{c}`")
                            for c in out.columns])
    return _transform


def drop_empty_rows(metric_cols: Sequence[str]):
    """Drops rows where every metric is NULL (coalesce returns the first non-NULL value)."""
    def _transform(df: DataFrame) -> DataFrame:
        return df.filter(F.coalesce(*[F.col(f"`{c}`") for c in metric_cols]).isNotNull())
    return _transform


def dedupe_latest(keys: Sequence[str], order_col: str = BRONZE_LOAD_COL):
    """Keeps one row per key: the most recently loaded Bronze row (late corrections win)."""
    def _transform(df: DataFrame) -> DataFrame:
        w = Window.partitionBy(*[F.col(f"`{k}`") for k in keys]).orderBy(F.col(order_col).desc_nulls_last(),
                                                                           F.col("source_file").desc_nulls_last())
        return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return _transform


def quarantine_frame(bad: DataFrame, level: str, contract: Sequence[str], rescued_col: str, batch_id: str) -> DataFrame:
    """Shape quarantined rows for silver.quarantine_records (the full raw row is kept as JSON)."""
    return bad.select(
        F.lit(level).alias("level"),
        F.col("Date").alias("date_raw"),
        F.col("RegionName").alias("region_raw"),
        F.col(REASON_COL).alias("reason_code"),
        F.create_map(*[x for k, v in REASONS.items() for x in (F.lit(k), F.lit(v))])[F.col(REASON_COL)].alias("reason_detail"),
        F.to_json(F.struct(*[F.col(f"`{c}`") for c in contract], F.col(rescued_col))).alias("raw_record"),
        F.col("source"), F.col("source_file"),
        F.current_timestamp().alias("quarantined_at"),
        F.lit(str(batch_id)).alias("batch_id"))


QUARANTINE_DDL = """CREATE TABLE IF NOT EXISTS {table} (
  level STRING COMMENT 'Geography level of the rejected row',
  date_raw STRING COMMENT 'Date exactly as received',
  region_raw STRING COMMENT 'RegionName exactly as received',
  reason_code STRING COMMENT 'NULL_KEY, BAD_DATE, INVALID_REGION, RESCUED_DATA or NON_NUMERIC',
  reason_detail STRING COMMENT 'Human-readable explanation of the reason code',
  raw_record STRING COMMENT 'Complete original row as JSON, for investigation and replay',
  source STRING COMMENT 'Audit: Bronze ingestion method the row came from',
  source_file STRING COMMENT 'Audit: source file of the row',
  quarantined_at TIMESTAMP COMMENT 'Audit: when the row was quarantined',
  batch_id STRING COMMENT 'Audit: job run id that quarantined the row'
) USING DELTA COMMENT 'Silver: rows rejected by data-quality rules, kept for investigation (never silently dropped)'"""


def silver_columns(mapping: Dict[str, str], contract: Sequence[str]) -> List[str]:
    """Final Silver column order: standardised contract columns, then audit columns."""
    return [mapping[c] for c in contract] + AUDIT_COLUMNS


def silver_table_ddl(full_table: str, contract: Sequence[str], mapping: Dict[str, str], comments: Dict[str, str],
                     table_comment: str, date_col: str = "Date", region_col: str = "RegionName") -> str:
    """CREATE TABLE IF NOT EXISTS with typed, commented columns (comments from the data dictionary)."""
    lines = []
    for c in contract:
        typ = "DATE" if c == date_col else ("STRING" if c == region_col else "DOUBLE")
        lines.append(f"  `{mapping[c]}` {typ} COMMENT {sql_string(comments.get(c, c) + f' (source column {c})')}")
    for c in AUDIT_COLUMNS:
        typ = "TIMESTAMP" if c == "load_dt" else "STRING"
        text = {"load_dt": "Audit: when the row was written to Silver",
                "source": "Audit/lineage: Bronze ingestion method the row came from",
                "source_file": "Audit/lineage: original chunk file of the row",
                "batch_id": "Audit: Silver job run id"}.get(c, AUDIT_COLUMN_COMMENTS[c])
        lines.append(f"  {c} {typ} COMMENT {sql_string(text)}")
    return (f"CREATE TABLE IF NOT EXISTS {full_table} (\n" + ",\n".join(lines) + "\n)\nUSING DELTA\n"
            f"COMMENT {sql_string(table_comment)}")


def finalize_silver(mapping: Dict[str, str], contract: Sequence[str], batch_id: str):
    """Rename headers, set Silver audit values, and order columns exactly as the table definition."""
    from .headers import standardize_headers

    def _transform(df: DataFrame) -> DataFrame:
        renamed = df.select(*[F.col(f"`{c}`") for c in contract], "source", "source_file").transform(
            standardize_headers(mapping))
        out = (renamed.withColumn("load_dt", F.current_timestamp())
                      .withColumn("batch_id", F.lit(str(batch_id))))
        return out.select(*silver_columns(mapping, contract))
    return _transform


def reconcile_silver(bronze_rows: int, quarantined: int, empty_dropped: int, duplicates: int,
                     silver_rows: int) -> Optional[str]:
    """Every Bronze row must be accounted for. Returns an error message, or None when it balances."""
    accounted = quarantined + empty_dropped + duplicates + silver_rows
    if accounted != bronze_rows:
        return (f"Bronze {bronze_rows:,} != silver {silver_rows:,} + quarantined {quarantined:,} "
                f"+ empty {empty_dropped:,} + duplicates {duplicates:,} (= {accounted:,})")
    return None


# ---------------------------------------------------------------- reference (crosswalk) data
REFERENCE_RULES = {
    # standardised names; zero-padding restores codes that lost leading zeros
    "county_crosswalk": {"key": ["fips"], "pad": {"fips": 5, "state_fips": 2, "county_fips": 3}, "upper": []},
    "city_crosswalk": {"key": ["unique_city_id"], "pad": {}, "upper": ["state"]},
}


def clean_reference(mapping: Dict[str, str], rules: Dict, batch_id: str):
    """Crosswalk cleaning: snake_case headers, trim text, zero-pad codes, upper-case state codes,
    drop NULL keys and keep one row per key (latest Bronze load)."""
    from .headers import standardize_headers

    def _transform(df: DataFrame) -> DataFrame:
        contract = list(mapping)
        out = df.select(*[F.col(f"`{c}`") for c in contract], "source", "source_file",
                        F.col("load_dt").alias(BRONZE_LOAD_COL)).transform(standardize_headers(mapping))
        for c in [mapping[c] for c in contract]:
            out = out.withColumn(c, F.nullif(F.trim(F.col(c)), F.lit("")))
        for c, width in rules["pad"].items():
            out = out.withColumn(c, F.lpad(F.col(c), width, "0"))
        for c in rules["upper"]:
            out = out.withColumn(c, F.upper(F.col(c)))
        out = out.dropna(subset=rules["key"]).transform(dedupe_latest(rules["key"]))
        return (out.withColumn("load_dt", F.current_timestamp()).withColumn("batch_id", F.lit(str(batch_id)))
                   .select(*[mapping[c] for c in contract], *AUDIT_COLUMNS))
    return _transform
