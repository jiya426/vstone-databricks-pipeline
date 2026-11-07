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

## Benchmarks (accuracy) and monitoring - best practice
Genie answers are tested like code: each benchmark question has a ground-truth SQL and an evaluation note;
**Benchmark -> Run all benchmarks** compares Genie's answer with the ground truth.
**Acceptance threshold: 80 % of benchmark questions Good** - the agent is shared only above it, and the
benchmark is re-run after every change to instructions, tables or column comments.
Result on 07-11-2026: **5 / 5 Good (100 %) - passed.** With 5 questions one wrong answer costs 20 %, so the set
should grow to 10-20 questions covering top-N, single values, trends, comparisons and aggregates.

| Question | Ground-truth SQL | Evaluation note |
|---|---|---|
| Which 5 states had the highest typical home value in December 2017? | `SELECT region_name, home_value_usd FROM vstone.gold.agg_monthly_market_trend WHERE date = '2017-12-31' AND home_value_usd IS NOT NULL ORDER BY home_value_usd DESC LIMIT 5` | Same 5 states in the same order; extra columns or rounding are fine |
| What was California's typical home value in December 2017? | `SELECT home_value_usd FROM vstone.gold.agg_monthly_market_trend WHERE state_code = 'CA' AND date = '2017-12-31'` | Value matches; rounding to whole dollars is fine |
| Which state had the fastest year-over-year home value growth in December 2017? | `SELECT region_name, zhvi_yoy_pct FROM vstone.gold.agg_monthly_market_trend WHERE date = '2017-12-31' AND zhvi_yoy_pct IS NOT NULL ORDER BY zhvi_yoy_pct DESC LIMIT 1` | Same state; the percentage may be rounded |
| What was the average monthly rent in Texas in 2017? | `SELECT AVG(monthly_rent_usd) FROM vstone.gold.agg_monthly_market_trend WHERE state_code = 'TX' AND year = 2017` | Average matches within 1 dollar |
| Which ZIP code has the highest typical home value? | `SELECT region_name, home_value_usd FROM vstone.gold.agg_top10_regions_by_home_value WHERE level = 'zip' AND rank_in_level = 1` | Same ZIP code; extra ranked rows are fine if the first matches |

When a benchmark turns Bad: open it, see what Genie misunderstood, add a line to **Instructions**, re-run.

**Monitor** tab: every question asked, the SQL Genie ran and user feedback (thumbs up/down) - reviewed to find
questions Genie struggles with, which become new instructions or benchmarks.
