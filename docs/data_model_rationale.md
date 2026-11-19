# Gold data model: design decisions and rationale

Status: **target design**. Items 1, 2 and 3 of section 8 are **live** on `dev` (verified in Databricks on 2026-11-13: `dim_metric` has no history columns, 96 rows and 96 keys; `region_sk` is the primary key of `dim_region_current` and the fact points to it, pipeline run #75). Items 4 and 5 are planned, and the point-in-time join in item 2 is not yet built.

Reviewed against the Silver data and the current Gold build (`docs/gold_data_model.md` describes what is built today). Each decision below says what the build does now,
what the target is, and why.

## 1. Inputs that drive the design

| Fact about the data | Measured value | Design consequence |
|---|---|---|
| Volume | 10,584,674 Silver rows; about 10.5 M monthly values after normalisation | Facts are large, so keys must be compact (integers) and the fact must be clustered on its join keys |
| Variety of grain | 6 geography levels; money metrics exist at all levels, sales metrics only at state level | Sparse, mixed-grain data: one fact cannot describe every metric without many empty combinations |
| Variety of units | USD, USD per month, percent, counts, days, ratios | One numeric column holding mixed units makes SUM and AVG across metrics meaningless |
| History | Crosswalks change: counties move between metros (CBSA), names are corrected | Region attributes change over time, so the region dimension needs history |
| Late data | Late-arriving ZIP revisions (Day 7) | Facts must be re-computed or merged; the dimensions must absorb corrections |

## 2. Schema choice: star, not snowflake

**Decision: star schema.** Each dimension is one flat table, joined directly to the fact.

Why not snowflake: a snowflake would split `dim_region` into state, metro, county and city tables. Every
dashboard query would then add joins, and the BI tool would have to know the hierarchy. The gain (less duplicated
text) is small here: region names are short and the region dimension has about 30,000 rows. The rule we follow:
normalise only when the saving is large or the sub-entity is shared by other entities. Neither applies.

## 3. Fact tables: two, not one

**Decision: two fact tables that share the same conformed dimensions.**

| Fact | Grain | Metrics | Why this grouping |
|---|---|---|---|
| `fact_market_value_monthly` | region x month x value metric | ZHVI, ZRI, median listing price (all USD or USD per month) | Same unit family, dense across all six levels, most used in dashboards |
| `fact_market_activity_monthly` | region x month x activity metric | sales counts, inventory, days on Zillow, percent metrics | Different units and sparser (sales are state-level only); mixing them in one fact forces one measure column to hold counts, days and percents |

Current build: one fact `fact_housing_metric_monthly` (region x metric x month) holding everything in one
`metric_value` column, with `unit` as a descriptive column. It is correct and validated, but it breaks the rule
that a measure column should have one meaning. The split is the target; it is listed in section 8.

## 4. Dimensions and surrogate keys

| Dimension | Change type | Surrogate key | Business key | Why |
|---|---|---|---|---|
| `dim_date` | **Type 0** (static, never changes) | `date_key` INT yyyyMMdd | the date | Calendar facts do not change. Versioning a calendar is wasted work, so this is not SCD2 |
| `dim_region` | **Type 2** (history kept) | `region_sk` BIGINT, one per version (hash of `region_key` and version start) | `region_code` within `level` | A county's metro (CBSA) and a city's county change over time. Reporting "2015 sales by metro" must use the metro that existed in 2015 |
| `dim_metric` | **Type 1** (overwrite) | `metric_key` (business key; a `metric_sk` is deferred with `region_sk`) | `metric_key` | A metric's definition is corrected in place. History of a definition is not needed for analysis |

**Why surrogate keys on the dimensions:**
- A Type 2 dimension has several rows per business key, so the business key cannot be the primary key of the
  history table. The surrogate key (`region_sk`) identifies one version.
- Integer keys join faster and take less space than strings on a 10 M-row fact.
- Business keys stay as attributes, so the source can still be traced.

**Why not on the fact:** the fact has no natural surrogate key. Its primary key is the combination of its
dimension keys, which keeps the grain explicit. As built: (region_sk, metric_key, date_key). Target: (region_sk, metric_sk, date_key).

**Limit of the version dates:** the version start of each region is the pipeline run time, not the date the region
changed. The source (Zillow crosswalk) has no change dates, so `region_sk` identifies a version but cannot answer
"what did this county look like on date X" from the data.

**Current build vs target:** the fact now carries `region_sk` (built), taken from `dim_region_current` at load, so
every fact row points at the current version. The SCD2 history is kept in `dim_region` as an audit trail. The target
is a point-in-time join, where each fact row points at the region version valid for its month. That is not built
yet, and the limitation is stated in the presentation.

## 5. Slowly changing dimensions: which table, which type

- `dim_region` is Type 2 because its hierarchy changes and reports must reflect the hierarchy of their month.
- `dim_metric` is Type 1 because definitions are corrections, not events.
- `dim_date` is Type 0 because it is generated once.
- The fact has no SCD: facts are events or measurements, and a corrected value replaces the old one through the
  Silver MERGE (Day 7), not through a dimension.

## 6. Change data capture (CDC)

| Layer | CDC mechanism | Why |
|---|---|---|
| Bronze | Auto Loader (`availableNow`) and COPY INTO file tracking | Files arrive as batches. Tracking files, not rows, is the natural change unit |
| Silver | MERGE INTO with null-safe change detection (`<=>`) | Only rows whose values actually changed are updated. Idempotent on re-run |
| Gold dimensions | Snapshot CDC (`AUTO CDC ... FROM SNAPSHOT`) | The source does not publish a change feed. Comparing the new snapshot with the previous one finds the changes |
| Gold facts | Recomputed as a materialized view from Silver | The fact is derived in full; recomputation is simpler and correct for the volume (minutes, not hours) |

**Not implemented:** Delta Change Data Feed on Silver. It would let Gold read only changed rows, but at this
volume the full recompute is cheaper to operate than the extra table properties. This would change above roughly
50 M rows or a sub-hourly refresh.

## 7. Keys and relationships

| Table | Primary key | Foreign keys | Relationship |
|---|---|---|---|
| `dim_date` | `date_key` | none | referenced by both facts, 1 to many |
| `dim_region` (as built: `dim_region_current`) | `region_sk` | none | referenced by the fact, 1 to many |
| `dim_metric` | `metric_sk` | none | referenced by both facts, 1 to many |
| `fact_market_value_monthly` (target; as built: `fact_housing_metric_monthly`) | (region_sk, metric_sk, date_key) | region_sk, metric_sk, date_key | many to one to each dimension |
| `fact_market_activity_monthly` (target) | (region_sk, metric_sk, date_key) | region_sk, metric_sk, date_key | many to one to each dimension |

Primary keys are declared as constraints (`declare_constraints` in config). Foreign keys are declared so the
BI tool can infer joins and so the data-quality check can confirm that no fact row points to a missing dimension.

## 8. Changes from the current build, in order

1. `dim_date`: generate as a static table (Type 0), removing the SCD2 snapshot flow. **Live (run #66).**
2. `dim_region`: add `region_sk` per version. **Live (run #75, 2026-11-13):** `region_sk` is the key of `dim_region_current`, the fact carries it. **Not yet:** pointing each fact row at the version valid for its month.
3. `dim_metric`: switch to Type 1. **Live (run #66, 2026-11-13).** The business key `metric_key` stays; a `metric_sk` is added together with `region_sk` in step 2.
4. Split the fact into `fact_market_value_monthly` and `fact_market_activity_monthly`.
5. Re-point the dashboards: the market pages read the value fact, the sales pages read the activity fact.
6. Re-run the Gold job, the benchmark (Day 7) and the security self-tests; re-verify each dashboard page.

Items 1 to 3 are contained in the Gold pipeline. Item 4 changes the fact tables and therefore the dashboards
(item 5). Item 6 is required before the change is considered done.

## 9. Aggregates (not part of the star)

Aggregates are built on top of the facts for BI speed, not as dimensions:
`agg_top10_regions_by_home_value`, `agg_monthly_market_trend`, `agg_state_sales_summary`,
`agg_market_churn`, `agg_market_churn_summary`. They are materialized views and are recomputed with the facts, so
they can never disagree with them. The secured views (`market_trend_secured`, `zip_home_values_secured`,
`v_state_sales_by_access`) sit on top of the aggregates and enforce row and column security.

## 10. Summary of the decisions

| Question | Answer |
|---|---|
| Star or snowflake? | Star. Flat dimensions, fewer joins, the data does not justify normalisation |
| How many facts? | Two, split by unit family and grain (target); one in the current build |
| Which dimension is SCD2? | `dim_region` only |
| Surrogate keys where? | Region (`region_sk`, as built), metric (`metric_sk`, target) and the date (`date_key`); not on the facts |
| Is CDC used? | Yes: Auto Loader and MERGE in Bronze/Silver; snapshot CDC for dimensions; full recompute for facts |
| Primary keys | Dimension surrogates, and the fact's dimension keys (as built: `region_sk`, `metric_key`, `date_key` on the fact) |
| Foreign keys | Fact to each dimension |
