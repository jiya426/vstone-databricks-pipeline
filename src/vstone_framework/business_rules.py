"""Silver business rules (PDF Day 5: "Apply business rules (currency normalization, date standardization)").

1. Metric catalogue   every metric gets a family, segment, unit, currency, scale and precision
                      (from DataDictionary + conf "business_rules"); stored as silver.metric_catalog.
2. Value normalisation   metric_value = round(raw x fx_to_reporting x scale, decimals) as DECIMAL(18,4)
                         - currency units converted to the reporting currency (USD -> USD rate 1.0 today;
                           a new source currency only needs an fx entry in config)
                         - InventoryTierShare is 0-1 in the source, scaled to 0-100 like every other percent
                         - precision per unit (money 2 dp, counts 0 dp, days 1 dp, ratios/percents 4 dp)
3. Date standardisation  date (DATE) -> date_key yyyyMMdd INT, year, quarter, month, month_start_date
4. Region standardisation region_code (county FIPS zero-padded to 5), region_key = '<level>:<code>'
5. Unpivot              wide Silver (one column per metric) -> long (one row per region, month, metric);
                        NULL metric cells are dropped here (they mean "not reported").
"""
from __future__ import annotations

from typing import Dict, List, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .headers import to_snake_case
from .metadata import _match_family, column_comment

CATALOG_COLUMNS = ["metric_name", "metric_code", "metric_family", "home_segment", "unit", "currency_code",
                   "scale_factor", "decimal_places", "is_seasonally_adjusted", "is_msa_level", "definition"]


def classify_metric(code: str, dictionary: Dict[str, str], rules: Dict) -> Dict:
    """Catalogue entry for one source metric column, e.g. 'MedianListingPricePerSqft_3Bedroom'."""
    match = _match_family(code, dictionary)
    if match is None:
        raise ValueError(f"No data-dictionary family for metric {code}")
    family, segment = match
    unit = rules["unit_by_family"].get(family)
    if unit is None:
        raise ValueError(f"No unit rule for family {family} (metric {code})")
    lowered = code.lower()
    return {
        "metric_name": to_snake_case(code),
        "metric_code": code,
        "metric_family": family,
        "home_segment": (segment.replace("_Msa", "").replace("Msa", "") or "AllHomes") if segment else "AllHomes",
        "unit": unit,
        "currency_code": rules["reporting_currency"] if unit in rules["currency_units"] else None,
        "scale_factor": float(rules.get("scale_by_family", {}).get(family, 1.0)),
        "decimal_places": int(rules["decimal_places"][unit]),
        "is_seasonally_adjusted": any(t in lowered for t in ("seasonallyadjusted", "seasadj", "seas_adj"))
                                  or family in ("ZHVI", "ZRI"),          # ZHVI/ZRI are smoothed, SA by definition
        "is_msa_level": lowered.endswith("_msa"),
        "definition": column_comment(code, dictionary),
    }


def metric_catalog(codes: Sequence[str], dictionary: Dict[str, str], rules: Dict) -> List[Dict]:
    return [classify_metric(c, dictionary, rules) for c in sorted(set(codes))]


def normalized_value(raw: Column, unit: Column, scale: Column, rules: Dict) -> Column:
    """round(raw x fx x scale, decimals-for-unit) as DECIMAL(18,4)."""
    fx = F.lit(float(rules["fx_to_reporting"][rules["source_currency"]]))
    value = raw * scale
    value = F.when(unit.isin(*rules["currency_units"]), value * fx).otherwise(value)
    rounded = None
    for u, dp in rules["decimal_places"].items():
        rounded = (F.when(unit == u, F.round(value, dp)) if rounded is None
                   else rounded.when(unit == u, F.round(value, dp)))
    return rounded.otherwise(value).cast("decimal(18,4)")


def region_code(level: str, region_col: Column, rules: Dict) -> Column:
    """Standardised region code: county FIPS zero-padded (1003 -> 01003); other levels unchanged."""
    width = rules.get("region_code_padding", {}).get(level)
    return F.lpad(region_col, width, "0") if width else region_col


def with_date_attributes(date_col: str = "date"):
    """Adds date_key (yyyyMMdd INT), year, quarter, month, month_start_date."""
    def _transform(df: DataFrame) -> DataFrame:
        d = F.col(date_col)
        return (df.withColumn("date_key", F.date_format(d, "yyyyMMdd").cast("int"))
                  .withColumn("year", F.year(d))
                  .withColumn("quarter", F.quarter(d))
                  .withColumn("month", F.month(d))
                  .withColumn("month_start_date", F.trunc(d, "month")))
    return _transform


def unpivot_metrics(level: str, metric_cols: Sequence[str], rules: Dict):
    """Wide Silver -> long rows (level, region_code, region_key, date, metric_name, raw_value); NULLs dropped."""
    def _transform(df: DataFrame) -> DataFrame:
        pairs = ", ".join(f"'{c}', CAST(`{c}` AS DOUBLE)" for c in metric_cols)
        long = df.select("date", "region_name", "source", "source_file",
                         F.expr(f"stack({len(metric_cols)}, {pairs}) AS (metric_name, raw_value)"))
        code = region_code(level, F.col("region_name"), rules)
        return (long.filter(F.col("raw_value").isNotNull())
                    .withColumn("level", F.lit(level))
                    .withColumn("region_code", code)
                    .withColumn("region_key", F.concat(F.lit(f"{level}:"), code)))
    return _transform


LONG_COLUMNS = ["level", "region_code", "region_key", "date", "date_key", "year", "quarter", "month",
                "month_start_date", "metric_name", "unit", "currency_code", "metric_value", "raw_value",
                "load_dt", "source", "source_file", "batch_id"]


def apply_catalog(catalog: DataFrame, rules: Dict, batch_id: str):
    """Join the metric catalogue, normalise values, add date attributes and audit columns."""
    def _transform(long: DataFrame) -> DataFrame:
        joined = long.join(F.broadcast(catalog.select("metric_name", "unit", "currency_code", "scale_factor")),
                           "metric_name", "left")
        return (joined.withColumn("metric_value", normalized_value(F.col("raw_value"), F.col("unit"),
                                                                   F.col("scale_factor"), rules))
                      .transform(with_date_attributes())
                      .withColumn("load_dt", F.current_timestamp())
                      .withColumn("batch_id", F.lit(str(batch_id)))
                      .select(*LONG_COLUMNS))
    return _transform


def non_null_cells(df: DataFrame, metric_cols: Sequence[str]) -> int:
    """Expected long-table rows for a wide table: number of non-NULL metric cells (reconciliation)."""
    row = df.agg(*[F.count(F.col(f"`{c}`")).alias(f"c{i}") for i, c in enumerate(metric_cols)]).first()
    return int(sum(row))


def state_name_lookup(county_crosswalk: DataFrame) -> DataFrame:
    """compact state name ('NewYork') -> full name ('New York'): the State file stores names without spaces."""
    return (county_crosswalk.select("state_name").distinct()
            .withColumn("compact", F.regexp_replace("state_name", " ", "")))


def state_code_col(state_name: Column, rules: Dict) -> Column:
    pairs = [x for name, code in rules["state_codes"].items() for x in (F.lit(name), F.lit(code))]
    return F.create_map(*pairs)[state_name]


def code_to_state_name(code: Column, rules: Dict) -> Column:
    pairs = [x for name, c in rules["state_codes"].items() for x in (F.lit(c), F.lit(name))]
    return F.create_map(*pairs)[code]
