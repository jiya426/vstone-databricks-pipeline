# Incremental load, MERGE INTO and SCD Type 2 (Day 7)

**Job:** `vstone_incremental_load` (GitHub Actions -> Run workflow -> `incremental_load`).

## Scenario (simulated - the Zillow extract ends in Dec 2017)
| Rows | What | Expected effect |
|---|---|---|
| 100 ZIP codes, Dec 2017 | home value (ZHVI) revised by +1 % | MERGE **updates** 100 Silver rows |
| same 100 ZIP codes, Jan 2018 | next month arrives | MERGE **inserts** 100 Silver rows |
| county `01003` (Baldwin, AL) | crosswalk name corrected to "Baldwin County" | Gold `dim_region` gets a 2nd version |

All values come from real Bronze rows (config `late_arriving`); every late row carries `source = autoloader_late_csv`.

## Flow
1. **07b_late_arriving_bronze** - lands ONE new CSV in the `late_arriving` Volume (skipped if the batch exists) and
   appends only new files to `bronze.zillow_ts_late_arriving` with Auto Loader (checkpoint = exactly once).
2. **07c_late_arriving_silver_merge** - same Silver rules as the full load, then
   `MERGE INTO silver.zip_time_series ... WHEN MATCHED AND <value changed> THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`.
   Re-running changes 0 rows. A second MERGE applies the crosswalk correction.
3. **05a / 05b** - refresh the conformed Silver tables (long table, region reference, features).
4. **Gold pipeline** - AUTO CDC from snapshot sees the new county name: the old `dim_region` version is closed
   (`__END_AT` set) and a new current version opens; the new month enters `dim_date`; the fact picks up the values.
5. **06z_gold_checks** and **07d_incremental_evidence** - checks in every layer.

## Design choices
* **Full rebuild = incremental result.** The full Silver rebuild (04a) also reads `bronze.zillow_ts_late_arriving`
  and keeps the latest load per key, so a later full run gives the same Silver as the MERGE.
* **"Latest month" for rankings and churn** = the latest month reported by at least 50 % of a level's regions
  (config `gold.min_month_coverage`), so 100 early ZIP rows for Jan 2018 do not become the ranking month.
* **SCD2 demo is reversible and recorded.** The crosswalk correction is applied in Silver only. A later full Silver
  rebuild restores the source name from Bronze, and SCD2 records that as a further version - history is never lost.

## Evidence to capture
* `DESCRIBE HISTORY vstone.silver.zip_time_series` - the MERGE version with `numTargetRowsUpdated` / `Inserted` = 100 / 100.
* `SELECT * FROM vstone.gold.dim_region WHERE region_key = 'county:01003'` - 2 versions with `__START_AT` / `__END_AT`.
* The `incremental_evidence` task output - all checks PASS.
* A second run of the job: Auto Loader loads 0 rows and MERGE changes 0 rows (idempotent).
