# Business glossary

Generated from existing metadata (nothing typed by hand): metric definitions from the Zillow data dictionary
(`DataDictionary.csv` -> `silver.metric_catalog`), feature definitions from the Silver feature rules, level
definitions from `conf/vstone_config.json`. The same rows are stored in the governed table
`governance.business_glossary` by notebook `06b_business_glossary` (job **vstone_gold**).

110 terms: 96 metric, 6 feature, 6 geography level, 2 key.

## Geography level

| Term | Source name | Definition | Unit | Used in |
|---|---|---|---|---|
| `city` | city | City, identified by Zillow's unique city id |  | `dim_region_current.level` |
| `county` | county | US county, identified by its 5-digit FIPS code |  | `dim_region_current.level` |
| `metro` | metro | Metropolitan statistical area (CBSA) |  | `dim_region_current.level` |
| `neighborhood` | neighborhood | Neighborhood, identified by Zillow's region id |  | `dim_region_current.level` |
| `state` | state | US state (plus the national United States series) |  | `dim_region_current.level` |
| `zip` | zip | 5-digit ZIP code |  | `dim_region_current.level` |

## Key

| Term | Source name | Definition | Unit | Used in |
|---|---|---|---|---|
| `date_key` | date_key | Month (foreign key to dim_date_current) |  | `fact_housing_metric_monthly.date_key` |
| `region_key` | region_key | Business key '<level>:<region_code>' (primary key) |  | `dim_region_current.region_key` |

## Feature

| Term | Source name | Definition | Unit | Used in |
|---|---|---|---|---|
| `gross_rent_yield_pct` | gross_rent_yield_pct | Annual rent as a share of home value: ZRI x 12 / ZHVI x 100 (affordability / investor yield) | PERCENT | `agg_monthly_market_trend.gross_rent_yield_pct` |
| `listing_premium_pct` | listing_premium_pct | How far median asking price is above (+) or below (-) typical home value, percent | PERCENT | `agg_monthly_market_trend.listing_premium_pct` |
| `tier_spread_ratio` | tier_spread_ratio | Top-tier home value divided by bottom-tier home value (price inequality within the region) | RATIO | `agg_monthly_market_trend.tier_spread_ratio` |
| `zhvi_mom_pct` | zhvi_mom_pct | Month-over-month change in typical home value (ZHVI_AllHomes), percent | PERCENT | `agg_monthly_market_trend.zhvi_mom_pct` |
| `zhvi_yoy_pct` | zhvi_yoy_pct | Year-over-year change in typical home value (ZHVI_AllHomes), percent | PERCENT | `agg_monthly_market_trend.zhvi_yoy_pct` |
| `zri_yoy_pct` | zri_yoy_pct | Year-over-year change in typical rent (ZRI_AllHomes), percent | PERCENT | `agg_monthly_market_trend.zri_yoy_pct` |

## Metric

| Term | Source name | Definition | Unit | Used in |
|---|---|---|---|---|
| `age_of_inventory` | AgeOfInventory | Each Wednesday, age of inventory is calculated as the median number of days all active listings as of that Wednesday have been current. These medians are then aggregated into the number reported by taking the median across weekly values. | DAYS | `fact_housing_metric_monthly.metric_key` |
| `days_on_zillow_all_homes` | DaysOnZillow_AllHomes | The median days on market of homes sold within a given month, including foreclosure re-sales. The latest data is for one month prior to the current ZHVI (e.g., if the most current month for ZHVI data is January, the most current month for Days on Zillow data will be December). Segment: AllHomes. | DAYS | `fact_housing_metric_monthly.metric_key` |
| `inventory_raw_all_homes` | InventoryRaw_AllHomes | Median of weekly snapshot of for-sale homes within a region for a given month Segment: AllHomes. | COUNT | `fact_housing_metric_monthly.metric_key` |
| `inventory_seasonally_adjusted_all_homes` | InventorySeasonallyAdjusted_AllHomes | A seasonally adjusted measure of the median of weekly snapshot of for-sale homes within a region for a given month Segment: AllHomes. | COUNT | `fact_housing_metric_monthly.metric_key` |
| `inventory_seasonally_adjusted_bottom_tier` | InventorySeasonallyAdjusted_BottomTier | A seasonally adjusted measure of the median of weekly snapshot of for-sale homes within a region for a given month Segment: BottomTier. | COUNT | `fact_housing_metric_monthly.metric_key` |
| `inventory_seasonally_adjusted_middle_tier` | InventorySeasonallyAdjusted_MiddleTier | A seasonally adjusted measure of the median of weekly snapshot of for-sale homes within a region for a given month Segment: MiddleTier. | COUNT | `fact_housing_metric_monthly.metric_key` |
| `inventory_seasonally_adjusted_top_tier` | InventorySeasonallyAdjusted_TopTier | A seasonally adjusted measure of the median of weekly snapshot of for-sale homes within a region for a given month Segment: TopTier. | COUNT | `fact_housing_metric_monthly.metric_key` |
| `inventory_tier_share_bottom_tier_all_homes` | InventoryTierShare_BottomTier_AllHomes | The share of inventory available classified as Bottom, Middle, or Top tier according the asking price. Segment: BottomTier_AllHomes. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `inventory_tier_share_middle_tier_all_homes` | InventoryTierShare_MiddleTier_AllHomes | The share of inventory available classified as Bottom, Middle, or Top tier according the asking price. Segment: MiddleTier_AllHomes. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `inventory_tier_share_top_tier_all_homes` | InventoryTierShare_TopTier_AllHomes | The share of inventory available classified as Bottom, Middle, or Top tier according the asking price. Segment: TopTier_AllHomes. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_1_bedroom` | MedianListingPrice_1Bedroom | Median of the list price (or asking price) for homes listed on Zillow Segment: 1Bedroom. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_2_bedroom` | MedianListingPrice_2Bedroom | Median of the list price (or asking price) for homes listed on Zillow Segment: 2Bedroom. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_3_bedroom` | MedianListingPrice_3Bedroom | Median of the list price (or asking price) for homes listed on Zillow Segment: 3Bedroom. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_4_bedroom` | MedianListingPrice_4Bedroom | Median of the list price (or asking price) for homes listed on Zillow Segment: 4Bedroom. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_5_bedroom_or_more` | MedianListingPrice_5BedroomOrMore | Median of the list price (or asking price) for homes listed on Zillow Segment: 5BedroomOrMore. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_all_homes` | MedianListingPrice_AllHomes | Median of the list price (or asking price) for homes listed on Zillow Segment: AllHomes. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_condo_coop` | MedianListingPrice_CondoCoop | Median of the list price (or asking price) for homes listed on Zillow Segment: CondoCoop. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_duplex_triplex` | MedianListingPrice_DuplexTriplex | Median of the list price (or asking price) for homes listed on Zillow Segment: DuplexTriplex. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_per_sqft_1_bedroom` | MedianListingPricePerSqft_1Bedroom | Median of list prices divided by the square footage of a home Segment: 1Bedroom. | USD_PER_SQFT | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_per_sqft_2_bedroom` | MedianListingPricePerSqft_2Bedroom | Median of list prices divided by the square footage of a home Segment: 2Bedroom. | USD_PER_SQFT | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_per_sqft_3_bedroom` | MedianListingPricePerSqft_3Bedroom | Median of list prices divided by the square footage of a home Segment: 3Bedroom. | USD_PER_SQFT | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_per_sqft_4_bedroom` | MedianListingPricePerSqft_4Bedroom | Median of list prices divided by the square footage of a home Segment: 4Bedroom. | USD_PER_SQFT | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_per_sqft_5_bedroom_or_more` | MedianListingPricePerSqft_5BedroomOrMore | Median of list prices divided by the square footage of a home Segment: 5BedroomOrMore. | USD_PER_SQFT | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_per_sqft_all_homes` | MedianListingPricePerSqft_AllHomes | Median of list prices divided by the square footage of a home Segment: AllHomes. | USD_PER_SQFT | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_per_sqft_condo_coop` | MedianListingPricePerSqft_CondoCoop | Median of list prices divided by the square footage of a home Segment: CondoCoop. | USD_PER_SQFT | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_per_sqft_duplex_triplex` | MedianListingPricePerSqft_DuplexTriplex | Median of list prices divided by the square footage of a home Segment: DuplexTriplex. | USD_PER_SQFT | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_per_sqft_single_family_residence` | MedianListingPricePerSqft_SingleFamilyResidence | Median of list prices divided by the square footage of a home Segment: SingleFamilyResidence. | USD_PER_SQFT | `fact_housing_metric_monthly.metric_key` |
| `median_listing_price_single_family_residence` | MedianListingPrice_SingleFamilyResidence | Median of the list price (or asking price) for homes listed on Zillow Segment: SingleFamilyResidence. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_pct_of_price_reduction_all_homes` | MedianPctOfPriceReduction_AllHomes | Median of the percentage price reduction for homes with a price reduction during the month Segment: AllHomes. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `median_pct_of_price_reduction_condo_coop` | MedianPctOfPriceReduction_CondoCoop | Median of the percentage price reduction for homes with a price reduction during the month Segment: CondoCoop. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `median_pct_of_price_reduction_single_family_residence` | MedianPctOfPriceReduction_SingleFamilyResidence | Median of the percentage price reduction for homes with a price reduction during the month Segment: SingleFamilyResidence. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `median_price_cut_dollar_all_homes` | MedianPriceCutDollar_AllHomes | Median of the price reduction for homes with a price reduction during the month Segment: AllHomes. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_price_cut_dollar_condo_coop` | MedianPriceCutDollar_CondoCoop | Median of the price reduction for homes with a price reduction during the month Segment: CondoCoop. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_price_cut_dollar_single_family_residence` | MedianPriceCutDollar_SingleFamilyResidence | Median of the price reduction for homes with a price reduction during the month Segment: SingleFamilyResidence. | USD | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_1_bedroom` | MedianRentalPrice_1Bedroom | Median of the rental price of homes listed for rent on Zillow in a given region Segment: 1Bedroom. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_2_bedroom` | MedianRentalPrice_2Bedroom | Median of the rental price of homes listed for rent on Zillow in a given region Segment: 2Bedroom. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_3_bedroom` | MedianRentalPrice_3Bedroom | Median of the rental price of homes listed for rent on Zillow in a given region Segment: 3Bedroom. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_4_bedroom` | MedianRentalPrice_4Bedroom | Median of the rental price of homes listed for rent on Zillow in a given region Segment: 4Bedroom. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_5_bedroom_or_more` | MedianRentalPrice_5BedroomOrMore | Median of the rental price of homes listed for rent on Zillow in a given region Segment: 5BedroomOrMore. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_all_homes` | MedianRentalPrice_AllHomes | Median of the rental price of homes listed for rent on Zillow in a given region Segment: AllHomes. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_condo_coop` | MedianRentalPrice_CondoCoop | Median of the rental price of homes listed for rent on Zillow in a given region Segment: CondoCoop. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_duplex_triplex` | MedianRentalPrice_DuplexTriplex | Median of the rental price of homes listed for rent on Zillow in a given region Segment: DuplexTriplex. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_multi_family_residence_5_plus_units` | MedianRentalPrice_MultiFamilyResidence5PlusUnits | Median of the rental price of homes listed for rent on Zillow in a given region Segment: MultiFamilyResidence5PlusUnits. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_per_sqft_1_bedroom` | MedianRentalPricePerSqft_1Bedroom | Median of the rental price per square foot of homes listed for rent on Zillow in a given region Segment: 1Bedroom. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_per_sqft_2_bedroom` | MedianRentalPricePerSqft_2Bedroom | Median of the rental price per square foot of homes listed for rent on Zillow in a given region Segment: 2Bedroom. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_per_sqft_3_bedroom` | MedianRentalPricePerSqft_3Bedroom | Median of the rental price per square foot of homes listed for rent on Zillow in a given region Segment: 3Bedroom. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_per_sqft_4_bedroom` | MedianRentalPricePerSqft_4Bedroom | Median of the rental price per square foot of homes listed for rent on Zillow in a given region Segment: 4Bedroom. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_per_sqft_5_bedroom_or_more` | MedianRentalPricePerSqft_5BedroomOrMore | Median of the rental price per square foot of homes listed for rent on Zillow in a given region Segment: 5BedroomOrMore. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_per_sqft_all_homes` | MedianRentalPricePerSqft_AllHomes | Median of the rental price per square foot of homes listed for rent on Zillow in a given region Segment: AllHomes. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_per_sqft_condo_coop` | MedianRentalPricePerSqft_CondoCoop | Median of the rental price per square foot of homes listed for rent on Zillow in a given region Segment: CondoCoop. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_per_sqft_duplex_triplex` | MedianRentalPricePerSqft_DuplexTriplex | Median of the rental price per square foot of homes listed for rent on Zillow in a given region Segment: DuplexTriplex. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_per_sqft_multi_family_residence_5_plus_units` | MedianRentalPricePerSqft_MultiFamilyResidence5PlusUnits | Median of the rental price per square foot of homes listed for rent on Zillow in a given region Segment: MultiFamilyResidence5PlusUnits. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_per_sqft_single_family_residence` | MedianRentalPricePerSqft_SingleFamilyResidence | Median of the rental price per square foot of homes listed for rent on Zillow in a given region Segment: SingleFamilyResidence. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_per_sqft_studio` | MedianRentalPricePerSqft_Studio | Median of the rental price per square foot of homes listed for rent on Zillow in a given region Segment: Studio. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_single_family_residence` | MedianRentalPrice_SingleFamilyResidence | Median of the rental price of homes listed for rent on Zillow in a given region Segment: SingleFamilyResidence. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `median_rental_price_studio` | MedianRentalPrice_Studio | Median of the rental price of homes listed for rent on Zillow in a given region Segment: Studio. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `pct_of_homes_decreasing_in_values_all_homes` | PctOfHomesDecreasingInValues_AllHomes | The percentage of homes in an given region with values that have decreased in the past year Segment: AllHomes. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_homes_increasing_in_values_all_homes` | PctOfHomesIncreasingInValues_AllHomes | The percentage of homes in an given region with values that have increased in the past year Segment: AllHomes. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_homes_selling_for_gain_all_homes` | PctOfHomesSellingForGain_AllHomes | The percentage of homes in an area that sold for a price higher than the previous sale price Segment: AllHomes. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_homes_selling_for_loss_all_homes` | PctOfHomesSellingForLoss_AllHomes | The percentage of homes in an area that sold for a price lower than the previous sale price Segment: AllHomes. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_all_homes` | PctOfListingsWithPriceReductions_AllHomes | The percentage of current listings on Zillow with a price cut during the month Segment: AllHomes. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_bottom_tier` | PctOfListingsWithPriceReductions_BottomTier | The percentage of current listings on Zillow with a price cut during the month Segment: BottomTier. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_condo_coop` | PctOfListingsWithPriceReductions_CondoCoop | The percentage of current listings on Zillow with a price cut during the month Segment: CondoCoop. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_middle_tier` | PctOfListingsWithPriceReductions_MiddleTier | The percentage of current listings on Zillow with a price cut during the month Segment: MiddleTier. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_seas_adj_all_homes` | PctOfListingsWithPriceReductionsSeasAdj_AllHomes | A seasonally adjusted measure of the percentage of current listings on Zillow with a price cut during the month Segment: AllHomes. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_seas_adj_bottom_tier` | PctOfListingsWithPriceReductionsSeasAdj_BottomTier | A seasonally adjusted measure of the percentage of current listings on Zillow with a price cut during the month Segment: BottomTier. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_seas_adj_condo_coop` | PctOfListingsWithPriceReductionsSeasAdj_CondoCoop | A seasonally adjusted measure of the percentage of current listings on Zillow with a price cut during the month Segment: CondoCoop. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_seas_adj_middle_tier` | PctOfListingsWithPriceReductionsSeasAdj_MiddleTier | A seasonally adjusted measure of the percentage of current listings on Zillow with a price cut during the month Segment: MiddleTier. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_seas_adj_single_family_residence` | PctOfListingsWithPriceReductionsSeasAdj_SingleFamilyResidence | A seasonally adjusted measure of the percentage of current listings on Zillow with a price cut during the month Segment: SingleFamilyResidence. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_seas_adj_top_tier` | PctOfListingsWithPriceReductionsSeasAdj_TopTier | A seasonally adjusted measure of the percentage of current listings on Zillow with a price cut during the month Segment: TopTier. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_single_family_residence` | PctOfListingsWithPriceReductions_SingleFamilyResidence | The percentage of current listings on Zillow with a price cut during the month Segment: SingleFamilyResidence. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `pct_of_listings_with_price_reductions_top_tier` | PctOfListingsWithPriceReductions_TopTier | The percentage of current listings on Zillow with a price cut during the month Segment: TopTier. | PERCENT | `fact_housing_metric_monthly.metric_key` |
| `price_to_rent_ratio_all_homes` | PriceToRentRatio_AllHomes | This ratio is first calculated at the individual home level, where the estimated home value is divided by 12 times its estimated monthly rent price. The the median of all home-level price-to-rent ratios for a given region is then calculated. Segment: AllHomes. | RATIO | `fact_housing_metric_monthly.metric_key` |
| `sale_counts` | Sale_Counts | The number of homes sold during a given month in conservatively defined consumer-to-consumer transactions adjusted for latency. Note: ZIP code sales data is not latency adjusted, but is filtered. | COUNT | `fact_housing_metric_monthly.metric_key` |
| `sale_counts_msa` | Sale_Counts_Msa | The number of homes sold during a given month in conservatively defined consumer-to-consumer transactions adjusted for latency. Note: ZIP code sales data is not latency adjusted, but is filtered. Segment: Msa. | COUNT | `fact_housing_metric_monthly.metric_key` |
| `sale_counts_seas_adj` | Sale_Counts_Seas_Adj | A seasonally adjusted measure of th number of homes sold during a given month in conservatively defined consumer-to-consumer transactions adjusted for latency. Note: ZIP code sales data is not latency adjusted, but is filtered. | COUNT | `fact_housing_metric_monthly.metric_key` |
| `sale_counts_seas_adj_msa` | Sale_Counts_Seas_Adj_Msa | A seasonally adjusted measure of th number of homes sold during a given month in conservatively defined consumer-to-consumer transactions adjusted for latency. Note: ZIP code sales data is not latency adjusted, but is filtered. Segment: Msa. | COUNT | `fact_housing_metric_monthly.metric_key` |
| `sale_prices` | Sale_Prices | Median of the selling price for all homes sold in a given region | USD | `fact_housing_metric_monthly.metric_key` |
| `sale_prices_msa` | Sale_Prices_Msa | Median of the selling price for all homes sold in a given region Segment: Msa. | USD | `fact_housing_metric_monthly.metric_key` |
| `zhvi_1_bedroom` | ZHVI_1bedroom | A smoothed seasonally adjusted measure of the median estimated home value across a given region and housing type Segment: 1bedroom. | USD | `fact_housing_metric_monthly.metric_key` |
| `zhvi_2_bedroom` | ZHVI_2bedroom | A smoothed seasonally adjusted measure of the median estimated home value across a given region and housing type Segment: 2bedroom. | USD | `fact_housing_metric_monthly.metric_key` |
| `zhvi_3_bedroom` | ZHVI_3bedroom | A smoothed seasonally adjusted measure of the median estimated home value across a given region and housing type Segment: 3bedroom. | USD | `fact_housing_metric_monthly.metric_key` |
| `zhvi_4_bedroom` | ZHVI_4bedroom | A smoothed seasonally adjusted measure of the median estimated home value across a given region and housing type Segment: 4bedroom. | USD | `fact_housing_metric_monthly.metric_key` |
| `zhvi_5_bedroom_or_more` | ZHVI_5BedroomOrMore | A smoothed seasonally adjusted measure of the median estimated home value across a given region and housing type Segment: 5BedroomOrMore. | USD | `fact_housing_metric_monthly.metric_key` |
| `zhvi_all_homes` | ZHVI_AllHomes | A smoothed seasonally adjusted measure of the median estimated home value across a given region and housing type Segment: AllHomes. | USD | `fact_housing_metric_monthly.metric_key` |
| `zhvi_bottom_tier` | ZHVI_BottomTier | A smoothed seasonally adjusted measure of the median estimated home value across a given region and housing type Segment: BottomTier. | USD | `fact_housing_metric_monthly.metric_key` |
| `zhvi_condo_coop` | ZHVI_CondoCoop | A smoothed seasonally adjusted measure of the median estimated home value across a given region and housing type Segment: CondoCoop. | USD | `fact_housing_metric_monthly.metric_key` |
| `zhvi_middle_tier` | ZHVI_MiddleTier | A smoothed seasonally adjusted measure of the median estimated home value across a given region and housing type Segment: MiddleTier. | USD | `fact_housing_metric_monthly.metric_key` |
| `zhvi_per_sqft_all_homes` | ZHVIPerSqft_AllHomes | Median of the value of all homes per square foot. This number is calculated by taking the estimated home value for each home in a given region and dividing it by the home’s square footage. Segment: AllHomes. | USD_PER_SQFT | `fact_housing_metric_monthly.metric_key` |
| `zhvi_single_family_residence` | ZHVI_SingleFamilyResidence | A smoothed seasonally adjusted measure of the median estimated home value across a given region and housing type Segment: SingleFamilyResidence. | USD | `fact_housing_metric_monthly.metric_key` |
| `zhvi_top_tier` | ZHVI_TopTier | A smoothed seasonally adjusted measure of the median estimated home value across a given region and housing type Segment: TopTier. | USD | `fact_housing_metric_monthly.metric_key` |
| `zri_all_homes` | ZRI_AllHomes | A smoothed seasonally adjusted measure of the median estimated market rate rent across a given region and housing type Segment: AllHomes. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `zri_all_homes_plus_multifamily` | ZRI_AllHomesPlusMultifamily | A smoothed seasonally adjusted measure of the median estimated market rate rent across a given region and housing type Segment: AllHomesPlusMultifamily. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `zri_multi_family_residence_rental` | Zri_MultiFamilyResidenceRental | A smoothed seasonally adjusted measure of the median estimated market rate rent across a given region and housing type Segment: MultiFamilyResidenceRental. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `zri_per_sqft_all_homes` | ZriPerSqft_AllHomes | Median of the estimated monthly rent price of all homes, per square foot. This is calculated by taking the estimated rent price for a home and dividing it by the homes square footage Segment: AllHomes. | USD_PER_SQFT_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
| `zri_single_family_residence_rental` | Zri_SingleFamilyResidenceRental | A smoothed seasonally adjusted measure of the median estimated market rate rent across a given region and housing type Segment: SingleFamilyResidenceRental. | USD_PER_MONTH | `fact_housing_metric_monthly.metric_key` |
