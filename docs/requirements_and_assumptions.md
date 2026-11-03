# Vstone - Requirements, Dataset Profile and Assumptions

Source of truth: `project-vstone-v7-20260504.pdf` (v7.0). Full blueprint with checklist:
https://claude.ai/artifact/SZ8bdwRVaQrjS9jvMAJ7Rz

## 1. Scope
End-to-end Databricks pipeline (Free Edition, serverless only) using a medallion architecture
(Bronze -> Silver -> Gold) over CSV, JSON and XML inputs, governed by Unity Catalog (RLS, CLS,
masking), orchestrated by Databricks Jobs, deployed with Databricks Asset Bundles and GitHub Actions.

## 2. Dataset: Zillow Economics (`archive (14)`)
| File | Rows | Regions | RegionName meaning |
|---|---|---|---|
| Zip_time_series.csv | 4,383,885 | 19,106 | 5-digit ZIP |
| City_time_series.csv | 3,762,566 | 16,636 | city slug -> cities_crosswalk.Unique_City_ID |
| Neighborhood_time_series.csv | 1,695,038 | 7,449 | Zillow RegionID (no crosswalk) |
| County_time_series.csv | 518,791 | 2,701 | FIPS (258 lost leading zero) -> CountyCrossWalk.FIPS |
| Metro_time_series.csv | 211,182 | 911 | CBSA code -> CountyCrossWalk.CBSACode |
| State_time_series.csv | 13,212 | 52 | state name without spaces |
| CountyCrossWalk_Zillow.csv | 3,144 | - | reference, key FIPS |
| cities_crosswalk.csv | 25,341 | - | reference, key Unique_City_ID |
| DataDictionary.csv, all_available_metrics.json, fields_per_level.json | - | - | metadata (not ingested) |

All time series: monthly, 1996-04-30 to 2017-12-31 (261 months), no duplicate (Date, RegionName),
no null keys, headers exactly match `fields_per_level.json`.

## 3. Data-quality findings
1. 302 Neighborhood rows have a corrupted RegionName (pandas Series text) -> quarantine.
2. 258 County FIPS codes lost the leading zero -> lpad to 5.
3. State names have spaces removed -> map via crosswalk.
4. National rows (`UnitedStates`, `United_States`) mixed with regions -> level 'Country'.
5. 284 city slugs not in crosswalk -> keep, flag unresolved.
6. No crosswalk for ZIP / Neighborhood -> dimension keyed by code.
7. 1,355 negative seasonally adjusted price-reduction % cells -> valid, warn only.
8. 60-85% of metric cells null -> unpivot to long form in Silver.
9. UTF-8 BOM on CountyCrossWalk header -> stripped.

## 4. Assumptions
| ID | Assumption |
|---|---|
| A1 | The six time-series files are chunked by date: <=2005-12-31, <=2012-12-31, <=2015-12-31, rest (configurable). |
| A2 | Crosswalks are not chunked; they are copied with chunk 1 and loaded via COPY INTO. |
| A3 | Metadata files are not ingested; used for comments, glossary and schema validation. |
| A4 | Chunk 2 CSV -> DLT, chunk 4 -> XML (resolves "2nd part" vs "4th part" in the PDF). |
| A5 | "Transactions CSV" = chunk 1 of the time series. |
| A6 | Currency: all money is USD; normalised to DECIMAL with currency_code and unit. |
| A7 | Dates: string -> DATE, plus date_key yyyyMMdd. |
| A8 | "Top customers / sales by region / monthly trend" -> top regions by home value, sales by state/metro, monthly market trend. |
| A9 | "Churn" -> inactive regions (no data in last 6 months) and cooling markets (6 consecutive ZHVI declines). |
| A10 | Late-arriving data is simulated with a held-back slice applied via MERGE. |
| A11 | RLS/CLS use workspace groups and a state-access mapping (no SCIM in Free Edition). |
| A12 | All object names come from bundle variables (catalog `vstone`, prod `vstone_prod`). |
