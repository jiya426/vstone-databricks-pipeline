# Silver business rules (Day 5)

All rules are code in `src/vstone_framework/` and configuration in `conf/vstone_config.json -> business_rules`.

## 1. Currency normalisation and units (`business_rules.py`, table `silver.metric_catalog`)
Every one of the 96 metrics gets a **unit** from its data-dictionary family, measured against the real data:

| Unit | Families | Currency | Precision |
|---|---|---|---|
| USD | ZHVI, MedianListingPrice, MedianSoldPrice (Sale_Prices), MedianPriceCutDollar | USD | 2 dp |
| USD_PER_MONTH | ZRI, MedianRentalPrice | USD | 2 dp |
| USD_PER_SQFT | ZHVIPerSqft, MedianListingPricePerSqft | USD | 4 dp |
| USD_PER_SQFT_PER_MONTH | ZriPerSqft, MedianRentalPricePerSqft | USD | 4 dp |
| PERCENT (0-100) | PctOf..., MedianPctOfPriceReduction, InventoryTierShare | - | 4 dp |
| COUNT | Inventory, Sale_Counts | - | 0 dp |
| DAYS | DaysOnZillow, AgeOfInventory | - | 1 dp |
| RATIO | PriceToRentRatio | - | 4 dp |

`metric_value = round(raw x fx_to_reporting x scale, decimals) -> DECIMAL(18,4)`
* **fx_to_reporting**: the source is USD and the reporting currency is USD (rate 1.0). A new currency only needs
  an entry in `fx_to_reporting` - no code change.
* **scale**: measured ranges showed every percentage is 0-100 **except InventoryTierShare, which is 0-1**
  (0.137-1.0); it is scaled x100 so all percentages are comparable.
* **DECIMAL(18,4)** instead of floating point: exact values for money and stable sums.

## 2. Date standardisation
`date` (month-end DATE) -> `date_key` (yyyyMMdd INT, the Gold date-dimension key), `year`, `quarter`, `month`,
`month_start_date`.

## 3. Region standardisation (`regions.py`, table `silver.region_reference`)
* County FIPS zero-padded to 5 digits (`1003` -> `01003`); `region_key = '<level>:<region_code>'` unique across levels.
* State names without spaces (`NewYork`) mapped to `New York` + `NY`; national rows tagged `region_type = 'national'`.
* Names and hierarchy (city -> county -> metro -> state) from the cleaned crosswalks; neighborhoods and ZIPs have
  no crosswalk in the dataset and are flagged `is_resolved = false`.

## 4. Long table (`silver.region_metric_monthly`)
Wide Silver (one column per metric) -> one row per region, month and metric; NULL cells (not reported) are dropped.
Reconciliation: long rows per level = non-NULL metric cells in the wide table (`ops.silver_conformed_reconciliation`).
Liquid Clustering on `(level, metric_name, date_key)`.

## 5. Features (`features.py`, table `silver.region_features_monthly`)
zhvi_mom_pct, zhvi_yoy_pct, zri_yoy_pct, gross_rent_yield_pct, tier_spread_ratio, listing_premium_pct.
Lags only count when the earlier month is exactly 1 or 12 months back. Measured ranges on real data:
rent yield 1.5-43.8 %, tier spread up to 17x.

## Engineering notes
* An 80-way OR chain in the NON_NUMERIC rule and deduplicating after casting made Spark's optimiser run out of
  memory while planning. Fixed with one `exists(array(...))` expression and by deduplicating raw rows first;
  regression test `test_full_silver_materialises_quickly`. The full test suite went from ~5 min to ~1 min.
