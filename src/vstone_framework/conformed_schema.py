"""Column definitions (name, type, comment) for the Day 5 conformed Silver tables."""
from .features import FEATURES

AUDIT = [("load_dt", "TIMESTAMP", "Audit: when the row was written"),
         ("source", "STRING", "Audit/lineage: Bronze ingestion method (or rule set) the row came from"),
         ("source_file", "STRING", "Audit/lineage: original source file(s)"),
         ("batch_id", "STRING", "Audit: job run id")]

DATE_ATTRS = [("date", "DATE", "Month-end date of the observation"),
              ("date_key", "INT", "Standardised date key yyyyMMdd (joins to the Gold date dimension)"),
              ("year", "INT", "Calendar year"), ("quarter", "INT", "Calendar quarter 1-4"),
              ("month", "INT", "Calendar month 1-12"), ("month_start_date", "DATE", "First day of the month")]

REGION_KEYS = [("level", "STRING", "Geography level: state, metro, county, city, neighborhood, zip"),
               ("region_code", "STRING", "Standardised region code (county FIPS zero-padded to 5 digits)"),
               ("region_key", "STRING", "Business key '<level>:<region_code>', unique across levels")]

METRIC_CATALOG = [
    ("metric_name", "STRING", "Standardised (snake_case) metric name - business key"),
    ("metric_code", "STRING", "Original Zillow column name"),
    ("metric_family", "STRING", "Measure family from the Zillow data dictionary, e.g. ZHVI, MedianListingPrice"),
    ("home_segment", "STRING", "Home type / size / tier the metric describes, e.g. AllHomes, 3Bedroom, TopTier"),
    ("unit", "STRING", "USD, USD_PER_MONTH, USD_PER_SQFT, USD_PER_SQFT_PER_MONTH, PERCENT, COUNT, DAYS or RATIO"),
    ("currency_code", "STRING", "Reporting currency for money units (USD); NULL for non-money units"),
    ("scale_factor", "DOUBLE", "Multiplier applied during normalisation (100 for InventoryTierShare, else 1)"),
    ("decimal_places", "INT", "Precision the normalised value is rounded to"),
    ("is_seasonally_adjusted", "BOOLEAN", "True when the series is seasonally adjusted or smoothed"),
    ("is_msa_level", "BOOLEAN", "True for metro-level (MSA) sale series"),
    ("definition", "STRING", "Business definition from the Zillow data dictionary")]

LONG = REGION_KEYS + DATE_ATTRS + [
    ("metric_name", "STRING", "Standardised metric name (joins to silver.metric_catalog)"),
    ("unit", "STRING", "Unit of metric_value"),
    ("currency_code", "STRING", "Reporting currency for money units, else NULL"),
    ("metric_value", "DECIMAL(18,4)", "Normalised value: raw x fx-to-reporting x scale, rounded per unit"),
    ("raw_value", "DOUBLE", "Value exactly as in Silver before normalisation (for audit)")] + AUDIT

REGION = REGION_KEYS + [
    ("region_name", "STRING", "Display name, e.g. 'Baldwin, AL'; the code itself when no crosswalk exists"),
    ("region_type", "STRING", "regional, or national for the United States rows"),
    ("state_code", "STRING", "2-letter state code"), ("state_name", "STRING", "Full state name"),
    ("county_fips", "STRING", "5-digit county FIPS"), ("county_name", "STRING", "County name"),
    ("metro_cbsa_code", "STRING", "CBSA code of the metro"), ("metro_name", "STRING", "Metro name (Zillow)"),
    ("city_name", "STRING", "City name"),
    ("is_resolved", "BOOLEAN", "True when names were found in a crosswalk")] + AUDIT

FEATURE_TABLE = REGION_KEYS + DATE_ATTRS + [
    ("zhvi_all_homes", "DOUBLE", "Typical home value (ZHVI, all homes), USD"),
    ("zri_all_homes", "DOUBLE", "Typical monthly rent (ZRI, all homes), USD"),
    ("zhvi_top_tier", "DOUBLE", "Typical value of top-tier homes, USD"),
    ("zhvi_bottom_tier", "DOUBLE", "Typical value of bottom-tier homes, USD"),
    ("median_listing_price_all_homes", "DOUBLE", "Median asking price, USD"),
    ("sale_counts", "DOUBLE", "Number of homes sold in the month")] + \
    [(name, "DOUBLE", text) for name, text in FEATURES.items()] + AUDIT
