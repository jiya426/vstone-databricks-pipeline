"""Silver features (trainer: "Silver: clean, transform, validate and generate meaningful features").

Computed per region and month from the cleaned wide Silver table. Lags only count when the earlier
month really is 1 (or 12) months back, so gaps in a region's history never produce a wrong change.
"""
from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .business_rules import region_code, with_date_attributes

BASE_METRICS = ["zhvi_all_homes", "zri_all_homes", "zhvi_top_tier", "zhvi_bottom_tier",
                "median_listing_price_all_homes", "sale_counts"]

FEATURES: Dict[str, str] = {
    "zhvi_mom_pct": "Month-over-month change in typical home value (ZHVI_AllHomes), percent",
    "zhvi_yoy_pct": "Year-over-year change in typical home value (ZHVI_AllHomes), percent",
    "zri_yoy_pct": "Year-over-year change in typical rent (ZRI_AllHomes), percent",
    "gross_rent_yield_pct": "Annual rent as a share of home value: ZRI x 12 / ZHVI x 100 (affordability / investor yield)",
    "tier_spread_ratio": "Top-tier home value divided by bottom-tier home value (price inequality within the region)",
    "listing_premium_pct": "How far median asking price is above (+) or below (-) typical home value, percent",
}

FEATURE_COLUMNS = (["level", "region_code", "region_key", "date", "date_key", "year", "quarter", "month",
                    "month_start_date"] + BASE_METRICS + list(FEATURES)
                   + ["load_dt", "source", "source_file", "batch_id"])


def _col(df: DataFrame, name: str):
    return F.col(name) if name in df.columns else F.lit(None).cast("double")


def _pct_change(cur, prev, months_apart, n):
    return F.when((months_apart == n) & (prev != 0), F.round((cur / prev - 1) * 100, 4))


def compute_features(level: str, rules: Dict, batch_id: str):
    """Wide Silver table -> one feature row per (region, month) with a home value or rent."""
    def _transform(df: DataFrame) -> DataFrame:
        base = df.select("date", "region_name", "source", "source_file", *[_col(df, m).alias(m) for m in BASE_METRICS])
        base = base.filter(F.col("zhvi_all_homes").isNotNull() | F.col("zri_all_homes").isNotNull())
        w = Window.partitionBy("region_name").orderBy("date")
        zhvi, zri = F.col("zhvi_all_homes"), F.col("zri_all_homes")
        gap1 = F.months_between("date", F.lag("date", 1).over(w))
        gap12 = F.months_between("date", F.lag("date", 12).over(w))
        code = region_code(level, F.col("region_name"), rules)
        out = (base
               .withColumn("zhvi_mom_pct", _pct_change(zhvi, F.lag(zhvi, 1).over(w), gap1, 1))
               .withColumn("zhvi_yoy_pct", _pct_change(zhvi, F.lag(zhvi, 12).over(w), gap12, 12))
               .withColumn("zri_yoy_pct", _pct_change(zri, F.lag(zri, 12).over(w), gap12, 12))
               .withColumn("gross_rent_yield_pct", F.when(zhvi > 0, F.round(zri * 12 / zhvi * 100, 4)))
               .withColumn("tier_spread_ratio", F.when(F.col("zhvi_bottom_tier") > 0,
                                                       F.round(F.col("zhvi_top_tier") / F.col("zhvi_bottom_tier"), 4)))
               .withColumn("listing_premium_pct", F.when(zhvi > 0, F.round(
                   (F.col("median_listing_price_all_homes") / zhvi - 1) * 100, 4)))
               .withColumn("level", F.lit(level))
               .withColumn("region_code", code)
               .withColumn("region_key", F.concat(F.lit(f"{level}:"), code))
               .transform(with_date_attributes())
               .withColumn("load_dt", F.current_timestamp())
               .withColumn("batch_id", F.lit(str(batch_id))))
        return out.select(*FEATURE_COLUMNS)
    return _transform
