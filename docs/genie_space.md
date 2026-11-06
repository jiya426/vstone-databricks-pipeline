# Genie space on the Gold aggregate (PDF Day 6)

Genie lets business users ask questions in plain English; it writes the SQL against the tables we give it.
Creating the space is a one-time UI setup (it is not a job or pipeline, so it is not triggered from GitHub).

## Create the space
1. Databricks sidebar -> **Genie** -> **New**.
2. **Data**: add `vstone.gold.agg_monthly_market_trend` (main table). Optionally add
   `vstone.gold.agg_state_sales_summary` and `vstone.gold.agg_top10_regions_by_home_value`.
3. **Title**: `Vstone - US housing market trends`.
4. **Description**: `Monthly home values, rents, asking prices, homes sold and growth per US state, 1996-2017 (Zillow).`
5. **Instructions** (General instructions box):
   ```
   - home_value_usd is the typical home value (Zillow ZHVI, all homes) in USD.
   - monthly_rent_usd is the typical monthly rent (Zillow ZRI) in USD.
   - zhvi_yoy_pct / zhvi_mom_pct are year-over-year / month-over-month changes in home value, in percent.
   - gross_rent_yield_pct = annual rent / home value x 100.
   - "Latest" means the most recent date in the table.
   - Use state_code (e.g. CA) or region_name (e.g. California) to filter states.
   ```
6. **Sample questions** (Settings -> Sample questions):
   - Which 5 states had the highest typical home value in December 2017?
   - Show California's monthly home value trend from 2010 to 2017.
   - Which states had the fastest year-over-year home value growth in 2017?
   - Compare the rent yield of Texas and New York in 2017.
   - How many homes were sold in Florida each year?
7. Click **Save**.

## Evidence for the PPT
Ask the 5 sample questions, then screenshot each answer with its generated SQL (click **Show code**).
The column comments in Gold (from `gold_schema.py`) are what Genie reads to understand each column.
