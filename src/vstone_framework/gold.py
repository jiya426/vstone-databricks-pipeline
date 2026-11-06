"""Gold layer transformations - pure DataFrame functions used by the Gold DLT pipeline and unit tested locally.

Star schema
    dim_date / dim_region / dim_metric             SCD Type 2 history (AUTO CDC from a snapshot of Silver)
    dim_*_current                                  one row per key, PRIMARY KEY  <- referenced by the fact
    fact_housing_metric_monthly                    grain: region x metric x month, FOREIGN KEYs to the 3 dims
Aggregates (business questions from the PDF, mapped to housing - assumption A8)
    agg_top10_regions_by_home_value                "top customers"  -> top 10 regions by home value, per level
    agg_monthly_market_trend                       "monthly trend"  -> monthly market metrics per state (Genie)
    agg_state_sales_summary                        "sales by region" -> homes sold and prices per state and year
"""
from __future__ import annotations

from typing import Dict, List, Mapping, Optional, Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .gold_schema import (AGG_MARKET_TREND, AGG_STATE_SALES, AGG_TOP_REGIONS, DIM_DATE, DIM_METRIC, DIM_REGION, FACT,
                          column_names)
from .quality import CheckResult, _result

GOLD_SOURCE = "gold_dlt"
SCD_START, SCD_END = "__START_AT", "__END_AT"      # columns AUTO CDC adds to SCD Type 2 targets


def with_gold_audit(source_file: str, batch_id: str):
    """Transformation: the 4 audit columns every layer carries."""
    def _transform(df: DataFrame) -> DataFrame:
        return (df.withColumn("load_dt", F.current_timestamp())
                  .withColumn("source", F.lit(GOLD_SOURCE))
                  .withColumn("source_file", F.lit(source_file))
                  .withColumn("batch_id", F.lit(batch_id)))
    return _transform


def _select(df: DataFrame, columns) -> DataFrame:
    """Select and cast to the exact Gold schema (name, type) - the DLT schema must match the query."""
    return df.select(*[F.col(n).cast(t).alias(n) for n, t, _ in columns])


# ------------------------------------------------------------------ dimension snapshots (input of AUTO CDC)
# Snapshots hold business columns only: a load timestamp would differ every run and create a fake new version.
def dim_date_snapshot(bounds: DataFrame) -> DataFrame:
    """Calendar of month-ends from the first to the last month in Silver (no gaps). `bounds` = (min_date, max_date)."""
    months = bounds.select(F.explode(F.sequence(F.trunc("min_date", "month"), F.trunc("max_date", "month"),
                                                F.expr("INTERVAL 1 MONTH"))).alias("month_start_date"))
    d = F.last_day("month_start_date")
    out = (months.withColumn("date", d)
                 .withColumn("date_key", F.date_format(d, "yyyyMMdd").cast("int"))
                 .withColumn("year", F.year(d)).withColumn("quarter", F.quarter(d)).withColumn("month", F.month(d))
                 .withColumn("month_name", F.date_format(d, "MMMM"))
                 .withColumn("year_month", F.date_format(d, "yyyy-MM"))
                 .withColumn("quarter_label", F.concat(F.year(d).cast("string"), F.lit("-Q"), F.quarter(d).cast("string")))
                 .withColumn("is_year_end", F.month(d) == 12))
    return _select(out, DIM_DATE)


def dim_region_snapshot(region_reference: DataFrame) -> DataFrame:
    return _select(region_reference, DIM_REGION)


def dim_metric_snapshot(metric_catalog: DataFrame) -> DataFrame:
    return _select(metric_catalog.withColumnRenamed("metric_name", "metric_key"), DIM_METRIC)


def current_rows(scd2: DataFrame, columns) -> DataFrame:
    """The current version of every key of an SCD Type 2 table (open-ended __END_AT)."""
    return _select(scd2.filter(F.col(SCD_END).isNull()), columns)


# ------------------------------------------------------------------ fact
def fact_frame(long: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """Silver long table -> fact at grain region x metric x month."""
    out = long.withColumnRenamed("metric_name", "metric_key").transform(with_gold_audit(source_file, batch_id))
    return _select(out, FACT)


def _date_from_key(col: str = "date_key"):
    return F.to_date(F.col(col).cast("string"), "yyyyMMdd")


# ------------------------------------------------------------------ aggregates
def top_regions(fact: DataFrame, regions: DataFrame, metric_key: str, levels: Sequence[str], top_n: int,
                source_file: str, batch_id: str) -> DataFrame:
    """Top N regions by home value in the latest month of each level, with the year-over-year change."""
    hv = (fact.filter(F.col("metric_key") == metric_key)
              .join(regions.select("region_key", "level", "region_name", "state_code", "region_type"), "region_key")
              .filter(F.col("level").isin(list(levels)) & (F.col("region_type") != "national"))
              .withColumn("date", _date_from_key()))
    latest = hv.groupBy("level").agg(F.max("date").alias("date"))
    now = hv.join(latest, ["level", "date"])
    prev = hv.select("region_key", F.col("date").alias("prev_date"), F.col("metric_value").alias("prev_value"))
    now = now.join(prev, (now.region_key == prev.region_key) &
                   (prev.prev_date == F.last_day(F.add_months(now.date, -12))), "left").drop(prev.region_key)
    w = Window.partitionBy("level").orderBy(F.col("metric_value").desc(), F.col("region_key"))
    out = (now.withColumn("rank_in_level", F.row_number().over(w)).filter(F.col("rank_in_level") <= top_n)
              .withColumn("home_value_usd", F.col("metric_value"))
              .withColumn("home_value_yoy_pct", F.when(F.col("prev_value") > 0, F.round(
                  (F.col("metric_value") / F.col("prev_value") - 1) * 100, 4)).cast("double"))
              .transform(with_gold_audit(source_file, batch_id)))
    return _select(out, AGG_TOP_REGIONS)


def market_trend(features: DataFrame, regions: DataFrame, levels: Sequence[str], source_file: str,
                 batch_id: str) -> DataFrame:
    """Monthly market metrics + features for the trend levels (states) - the Genie table."""
    out = (features.filter(F.col("level").isin(list(levels)))
           .drop("load_dt", "source", "source_file", "batch_id")
           .join(regions.select("region_key", "region_name", "state_code", "region_type"), "region_key")
           .withColumnRenamed("zhvi_all_homes", "home_value_usd")
           .withColumnRenamed("zri_all_homes", "monthly_rent_usd")
           .withColumnRenamed("median_listing_price_all_homes", "median_listing_price_usd")
           .withColumnRenamed("sale_counts", "homes_sold")
           .transform(with_gold_audit(source_file, batch_id)))
    return _select(out, AGG_MARKET_TREND)


def state_sales(fact: DataFrame, regions: DataFrame, metrics: Mapping[str, str], level: str, source_file: str,
                batch_id: str) -> DataFrame:
    """Per state and year: homes sold, average sale price, home value and asking price."""
    wanted = list(metrics.values())
    rows = (fact.filter(F.col("metric_key").isin(wanted))
                .join(regions.filter((F.col("level") == level) & (F.col("region_type") != "national"))
                      .select("region_key", "state_code", "state_name"), "region_key")
                .withColumn("year", (F.col("date_key") / 10000).cast("int")))
    val = lambda key: F.when(F.col("metric_key") == metrics[key], F.col("metric_value"))
    out = (rows.groupBy("state_code", "state_name", "year").agg(
               F.countDistinct(F.when(F.col("metric_key") == metrics["sales_count"], F.col("date_key"))).alias("months_reported"),
               F.sum(val("sales_count")).alias("homes_sold"),
               F.avg(val("sale_price")).alias("avg_sale_price_usd"),
               F.avg(val("home_value")).alias("avg_home_value_usd"),
               F.avg(val("listing_price")).alias("avg_listing_price_usd"))
           .filter(F.col("state_code").isNotNull())
           .transform(with_gold_audit(source_file, batch_id)))
    return _select(out, AGG_STATE_SALES)


# ------------------------------------------------------------------ glossary
def glossary_rows(metric_catalog: Sequence[Mapping], features: Mapping[str, str], level_definitions: Mapping[str, str],
                  fact_table: str, trend_table: str, region_table: str) -> List[tuple]:
    """Business glossary: every metric (definitions from the Zillow data dictionary), every feature,
    every geography level and the keys of the star schema."""
    rows = [(m["metric_name"], m["metric_code"], "Metric", m["definition"], m["unit"],
             f"{fact_table}.metric_key", "Zillow data dictionary") for m in metric_catalog]
    rows += [(name, name, "Feature", text, "PERCENT" if name.endswith("_pct") else "RATIO",
              f"{trend_table}.{name}", "Vstone feature rules (Silver)") for name, text in features.items()]
    rows += [(lvl, lvl, "Geography level", text, None, f"{region_table}.level", "Vstone configuration")
             for lvl, text in level_definitions.items()]
    rows += [(n, n, "Key", c, None, f"{t}.{n}", "Vstone Gold data model")
             for t, cols in ((region_table, DIM_REGION[:1]), (fact_table, FACT[2:3])) for n, _, c in cols]
    return rows


# ------------------------------------------------------------------ checks
def gold_checks(fact: DataFrame, silver_long_rows: int, current: Mapping[str, DataFrame], history: Mapping[str, DataFrame],
                pks: Mapping[str, Sequence[str]], fks: Mapping[str, str], top: DataFrame, top_n: int,
                top_levels: Sequence[str], trend: DataFrame, sales: DataFrame, levels: Sequence[str],
                fact_name: str, region_name: str) -> List[CheckResult]:
    """Reconciliation, keys, referential integrity, SCD2 integrity and aggregate sanity.

    current/history: {table name: DataFrame}; pks: {table name: key columns};
    fks: {fact column: current dimension table name}.
    """
    r: List[CheckResult] = []
    n = fact.count()
    r.append(_result(fact_name, "row_count_matches_silver", silver_long_rows, n, n == silver_long_rows))
    for name, df in {fact_name: fact, **current}.items():
        dup = df.groupBy(*pks[name]).count().filter("count > 1").count()
        r.append(_result(name, "primary_key_unique", 0, dup, dup == 0))
        nulls = df.filter(" OR ".join(f"{k} IS NULL" for k in pks[name])).count()
        r.append(_result(name, "primary_key_not_null", 0, nulls, nulls == 0))
    for col, dim_name in fks.items():
        orphans = fact.select(col).distinct().join(current[dim_name].select(col), col, "left_anti").count()
        r.append(_result(fact_name, f"{col}_has_dimension_row", 0, orphans, orphans == 0))
    for name, df in history.items():
        key = pks[f"{name}_current"]
        bad = (df.groupBy(*key).agg(F.sum(F.col(SCD_END).isNull().cast("int")).alias("open"))
                 .filter("open != 1").count())
        r.append(_result(name, "scd2_one_current_version_per_key", 0, bad, bad == 0))
    found_levels = sorted(x[0] for x in fact.select("region_key").distinct()
                          .join(current[region_name].select("region_key", "level"), "region_key")
                          .select("level").distinct().collect())
    r.append(_result(fact_name, "all_levels_reach_gold", sorted(levels), found_levels, found_levels == sorted(levels)))
    per_level = {x["level"]: x["count"] for x in top.groupBy("level").count().collect()}
    ok = set(per_level) == set(top_levels) and all(0 < c <= top_n for c in per_level.values())
    r.append(_result("agg_top_regions", "top_n_per_level", f"<= {top_n} for {sorted(top_levels)}", per_level, ok))
    for name, df in (("agg_market_trend", trend), ("agg_state_sales", sales)):
        rows = df.count()
        r.append(_result(name, "has_rows", "> 0", rows, rows > 0))
    return r
