"""Column definitions (name, type, comment) and keys of the Gold star schema.

Used three ways: the DLT pipeline declares each table with this schema (comments + PRIMARY/FOREIGN KEY
constraints), the Gold checks verify the tables against it, and the ER diagram is generated from it.
Metric definitions in comments come from the Zillow data dictionary through silver.metric_catalog.
"""
from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

from .config import load_config
from .features import FEATURES
from .metadata import sql_string

Column = Tuple[str, str, str]

AUDIT: List[Column] = [
    ("load_dt", "TIMESTAMP", "Audit: when the Gold pipeline wrote the row"),
    ("source", "STRING", "Audit/lineage: rule set that produced the row (gold_dlt)"),
    ("source_file", "STRING", "Audit/lineage: Silver table(s) the row was built from"),
    ("batch_id", "STRING", "Audit: Gold pipeline id"),
]

DIM_DATE: List[Column] = [
    ("date_key", "INT", "Surrogate date key yyyyMMdd (primary key)"),
    ("date", "DATE", "Month-end date"),
    ("year", "INT", "Calendar year"),
    ("quarter", "INT", "Calendar quarter 1-4"),
    ("month", "INT", "Calendar month 1-12"),
    ("month_name", "STRING", "Month name, e.g. January"),
    ("month_start_date", "DATE", "First day of the month"),
    ("year_month", "STRING", "yyyy-MM, e.g. 2017-12"),
    ("quarter_label", "STRING", "Quarter label, e.g. 2017-Q4"),
    ("is_year_end", "BOOLEAN", "True for December"),
]

DIM_REGION_SOURCE: List[Column] = [
    ("region_key", "STRING", "Business key '<level>:<region_code>' (primary key)"),
    ("level", "STRING", "Geography level: state, metro, county, city, neighborhood, zip"),
    ("region_code", "STRING", "Standardised region code (county FIPS zero-padded to 5 digits)"),
    ("region_name", "STRING", "Display name, e.g. 'Baldwin, AL'; the code itself when no crosswalk exists"),
    ("region_type", "STRING", "regional, or national for the United States series"),
    ("state_code", "STRING", "2-letter state code"),
    ("state_name", "STRING", "Full state name"),
    ("county_fips", "STRING", "5-digit county FIPS (from the county crosswalk)"),
    ("county_name", "STRING", "County name (from the county crosswalk)"),
    ("metro_cbsa_code", "STRING", "CBSA code of the metro (from the county crosswalk)"),
    ("metro_name", "STRING", "Metro name (from the county crosswalk)"),
    ("city_name", "STRING", "City name (from the city crosswalk)"),
    ("is_resolved", "BOOLEAN", "True when names were found in a crosswalk"),
]
# dim_region_current adds region_sk: one key per version of a region (see gold.current_region_rows)
DIM_REGION: List[Column] = ([("region_sk", "BIGINT", "Surrogate key: one per version of the region (primary key)")]
                            + DIM_REGION_SOURCE)

DIM_METRIC: List[Column] = [
    ("metric_key", "STRING", "Standardised (snake_case) metric name (primary key)"),
    ("metric_code", "STRING", "Original Zillow column name"),
    ("metric_family", "STRING", "Measure family from the Zillow data dictionary, e.g. ZHVI"),
    ("home_segment", "STRING", "Home type / size / tier, e.g. AllHomes, 3Bedroom, TopTier"),
    ("unit", "STRING", "USD, USD_PER_MONTH, USD_PER_SQFT, USD_PER_SQFT_PER_MONTH, PERCENT, COUNT, DAYS or RATIO"),
    ("currency_code", "STRING", "Reporting currency for money units (USD); NULL otherwise"),
    ("scale_factor", "DOUBLE", "Multiplier applied during normalisation"),
    ("decimal_places", "INT", "Precision the value is rounded to"),
    ("is_seasonally_adjusted", "BOOLEAN", "True when the series is seasonally adjusted or smoothed"),
    ("is_msa_level", "BOOLEAN", "True for metro-level (MSA) sale series"),
    ("definition", "STRING", "Business definition from the Zillow data dictionary"),
]

FACT_SOURCE: List[Column] = [
    ("region_key", "STRING", "Region business key (the version is in region_sk)"),
    ("metric_key", "STRING", "Metric (foreign key to dim_metric_current)"),
    ("date_key", "INT", "Month (foreign key to dim_date_current)"),
    ("metric_value", "DECIMAL(18,4)", "Normalised value in the metric's unit (USD for money)"),
    ("unit", "STRING", "Unit of metric_value"),
    ("currency_code", "STRING", "USD for money units, else NULL"),
] + AUDIT
FACT: List[Column] = (FACT_SOURCE[:1]
                      + [("region_sk", "BIGINT", "Region version (foreign key to dim_region_current)")]
                      + FACT_SOURCE[1:])

AGG_TOP_REGIONS: List[Column] = [
    ("level", "STRING", "Geography level the ranking is for"),
    ("rank_in_level", "INT", "1 = highest typical home value in the level"),
    ("region_key", "STRING", "Region business key"),
    ("region_name", "STRING", "Region display name"),
    ("state_code", "STRING", "2-letter state code (when known)"),
    ("date", "DATE", "Latest month with data for the level"),
    ("home_value_usd", "DECIMAL(18,4)", "Typical home value (ZHVI, all homes), USD"),
    ("home_value_yoy_pct", "DOUBLE", "Change versus the same month one year earlier, percent"),
] + AUDIT

AGG_MARKET_TREND: List[Column] = [
    ("region_key", "STRING", "Region business key"),
    ("region_name", "STRING", "State name, or United States"),
    ("state_code", "STRING", "2-letter state code; NULL for the United States"),
    ("region_type", "STRING", "regional or national"),
    ("date", "DATE", "Month-end date"),
    ("date_key", "INT", "yyyyMMdd"),
    ("year", "INT", "Calendar year"),
    ("month", "INT", "Calendar month 1-12"),
    ("home_value_usd", "DOUBLE", "Typical home value (ZHVI, all homes), USD"),
    ("monthly_rent_usd", "DOUBLE", "Typical monthly rent (ZRI, all homes), USD"),
    ("median_listing_price_usd", "DOUBLE", "Median asking price, USD"),
    ("homes_sold", "DOUBLE", "Number of homes sold in the month"),
] + [(name, "DOUBLE", text) for name, text in FEATURES.items()] + AUDIT

AGG_STATE_SALES: List[Column] = [
    ("state_code", "STRING", "2-letter state code"),
    ("state_name", "STRING", "Full state name"),
    ("year", "INT", "Calendar year"),
    ("months_reported", "BIGINT", "Months with sales data in the year"),
    ("homes_sold", "DECIMAL(18,4)", "Total homes sold in the year"),
    ("avg_sale_price_usd", "DECIMAL(18,4)", "Average of the monthly median sale prices, USD"),
    ("avg_home_value_usd", "DECIMAL(18,4)", "Average typical home value (ZHVI), USD"),
    ("avg_listing_price_usd", "DECIMAL(18,4)", "Average median asking price, USD"),
] + AUDIT

GLOSSARY: List[Column] = [
    ("term", "STRING", "Business term (snake_case name as used in the tables)"),
    ("display_name", "STRING", "Name as it appears in the source or reports"),
    ("category", "STRING", "Metric, Feature, Geography level or Key"),
    ("definition", "STRING", "Plain-language definition"),
    ("unit", "STRING", "Unit of measure, when the term is a measure"),
    ("used_in", "STRING", "Gold tables / columns where the term appears"),
    ("source", "STRING", "Where the definition comes from"),
]

# Table -> (columns, primary key, foreign keys {column: (table key, column)})
# natural key of each SCD Type 2 table (AUTO CDC keys and the history checks); region_sk is only in the current table
BUSINESS_KEYS: Dict[str, Tuple[str, ...]] = {"dim_region": ("region_key",)}

KEYS: Dict[str, Tuple[List[Column], Tuple[str, ...], Dict[str, Tuple[str, str]]]] = {
    "dim_date_current": (DIM_DATE, ("date_key",), {}),
    "dim_region_current": (DIM_REGION, ("region_sk",), {}),
    "dim_metric_current": (DIM_METRIC, ("metric_key",), {}),
    "fact": (FACT, ("region_sk", "metric_key", "date_key"),
             {"region_sk": ("dim_region_current", "region_sk"),
              "metric_key": ("dim_metric_current", "metric_key"),
              "date_key": ("dim_date_current", "date_key")}),
    "agg_top_regions": (AGG_TOP_REGIONS, ("level", "rank_in_level"), {}),
    "agg_market_trend": (AGG_MARKET_TREND, ("region_key", "date_key"), {}),
    "agg_state_sales": (AGG_STATE_SALES, ("state_code", "year"), {}),
}


def table_name(key: str) -> str:
    return load_config()["gold"]["tables"][key]


def column_names(columns: Sequence[Column]) -> List[str]:
    return [c[0] for c in columns]


def dlt_schema(key: str, catalog: str, schema: str, with_constraints: bool = True) -> str:
    """Schema string for @dlt.table: typed, commented columns; key columns NOT NULL; PK and FK constraints."""
    columns, pk, fks = KEYS[key]
    lines = [f"{n} {t}{' NOT NULL' if with_constraints and n in pk else ''} COMMENT {sql_string(c)}"
             for n, t, c in columns]
    if with_constraints:
        name = table_name(key)
        lines.append(f"CONSTRAINT {name}_pk PRIMARY KEY ({', '.join(pk)})")
        for col, (ref_key, ref_col) in fks.items():
            lines.append(f"CONSTRAINT {name}_{col}_fk FOREIGN KEY ({col}) "
                         f"REFERENCES {catalog}.{schema}.{table_name(ref_key)}({ref_col})")
    return ",\n".join(lines)


def er_diagram_mermaid() -> str:
    """Mermaid ER diagram of the star schema, generated from KEYS (docs/gold_data_model.md)."""
    out = ["erDiagram"]
    for key in ("dim_date_current", "dim_region_current", "dim_metric_current", "fact"):
        columns, pk, fks = KEYS[key]
        out.append(f"    {table_name(key)} {{")
        for n, t, _ in columns:
            tag = " " + ", ".join(k for k, on in (("PK", n in pk), ("FK", n in fks)) if on) if (n in pk or n in fks) else ""
            out.append(f"        {t.split('(')[0].replace(',', '')} {n}{tag}")
        out.append("    }")
    for col, (ref_key, _) in KEYS["fact"][2].items():
        out.append(f"    {table_name(ref_key)} ||--o{{ {table_name('fact')} : {col}")
    return "\n".join(out)


TABLE_COMMENTS: Dict[str, str] = {
    "dim_date": "Gold: date dimension (month-ends). Type 0: static calendar, never versioned",
    "dim_region": "Gold: region dimension (6 geography levels, names from the crosswalks), SCD Type 2 history via AUTO CDC",
    "dim_metric": "Gold: metric dimension (96 Zillow metrics with unit and data-dictionary definition). Type 1: overwritten in place, one row per metric",
    "dim_date_current": "Gold: current row of every date - primary key date_key, referenced by the fact",
    "dim_region_current": "Gold: current row of every region - primary key region_key, referenced by the fact",
    "dim_metric_current": "Gold: current row of every metric - primary key metric_key, referenced by the fact",
    "fact": "Gold fact: one normalised value per region, metric and month (grain region x metric x month)",
    "agg_top_regions": "Gold aggregate: top 10 regions by typical home value (ZHVI) in the latest month, per geography level",
    "agg_market_trend": "Gold aggregate: monthly housing market trend per state - home value, rent, prices, sales and growth features (Genie source)",
    "agg_state_sales": "Gold aggregate: homes sold, sale price, home value and asking price per state and year",
    "glossary": "Governance: business glossary of every metric, feature, geography level and key used in Gold",
}


AGG_CHURN: List[Column] = [
    ("region_key", "STRING", "Region business key"),
    ("level", "STRING", "Geography level"),
    ("region_name", "STRING", "Region display name"),
    ("state_code", "STRING", "2-letter state code (when known)"),
    ("last_data_date", "DATE", "Last month with a home value for the region"),
    ("months_since_last_data", "INT", "Months between the region's last value and the latest month of its level"),
    ("consecutive_monthly_declines", "INT", "Months in a row the home value fell, up to the last month"),
    ("last_home_value_usd", "DECIMAL(18,4)", "Home value (ZHVI) in the last month, USD"),
    ("status", "STRING", "active, cooling (falling for N months) or inactive (no data for N months)"),
] + AUDIT

AGG_CHURN_SUMMARY: List[Column] = [
    ("level", "STRING", "Geography level"),
    ("state_code", "STRING", "2-letter state code; UNKNOWN when the level has no state"),
    ("regions", "BIGINT", "Regions with a home value series"),
    ("active_regions", "BIGINT", "Regions with status active"),
    ("cooling_regions", "BIGINT", "Regions with status cooling"),
    ("inactive_regions", "BIGINT", "Regions with status inactive"),
    ("churn_rate_pct", "DOUBLE", "(cooling + inactive) / regions x 100"),
] + AUDIT

KEYS["agg_churn"] = (AGG_CHURN, ("region_key",), {})
KEYS["agg_churn_summary"] = (AGG_CHURN_SUMMARY, ("level", "state_code"), {})
TABLE_COMMENTS["agg_churn"] = ("Gold aggregate: churn-style status of every region - active, cooling (home value falling "
                               "for N months) or inactive (no data for N months)")
TABLE_COMMENTS["agg_churn_summary"] = "Gold aggregate: churn rate per geography level and state"
