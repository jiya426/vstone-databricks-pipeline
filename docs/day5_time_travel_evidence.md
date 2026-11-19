# Day 5 - Delta Lake ACID and time travel evidence

Notebook: `src/notebooks/05c_delta_acid_time_travel.py`, run 2026-11-04 (run_id `manual`).
It works on a **DEEP CLONE** of `silver.state_time_series` (`vstone.ops.time_travel_demo`), so real tables are
never changed. Every step is asserted in code and stored in `vstone.ops.time_travel_evidence`.
Final output: **`All ACID and time-travel checks passed.`**

```sql
SELECT step, property, action, version, rows, value, result
FROM vstone.ops.time_travel_evidence WHERE run_id = 'manual' ORDER BY step, recorded_at;
```

| Step | Property | Action | Version | Result |
|---|---|---|---|---|
| 1 | History | `DESCRIBE HISTORY silver.zip_time_series` - each Silver job run is a version | - | versions listed with operation + row metrics |
| 2 | Versions | `DEEP CLONE silver.state_time_series` | v0 | baseline rows and California value recorded |
| 2 | Versions | `UPDATE`: California values wrongly x10 | v1 | bad data committed |
| 2 | Versions | `DELETE`: Texas rows removed by mistake | v2 | rows lost |
| 3 | Time travel | `VERSION AS OF 0` | reads v0 | original row count and California value |
| 3 | Time travel | `TIMESTAMP AS OF '2026-11-04 07:30:47'` | reads v0 | same as version 0 |
| 4 | Recovery | `RESTORE TABLE ... TO VERSION AS OF 0` | v3 | rows and values back to the baseline |
| 5 | **A**tomicity | append 5 rows, 1 violates `CHECK (zhvi_all_homes >= 0)` | v4 (the CHECK itself) | whole batch rejected, **0 rows added, no new version** |
| 6 | **C**onsistency | append with an unexpected column (no mergeSchema) | v4 | rejected by schema enforcement, **no new version** |
| 7 | **I**solation | reader pinned to version 4 while rows are deleted | reads v4 | pinned count unchanged; the live table changed |
| 8 | **D**urability | 6 committed versions in `_delta_log`; version 0 still readable | v0 | old version fully queryable |

## What this demonstrates
* **Time travel** gives an audit trail and point-in-time reads (by version or timestamp) - e.g. reproducing a
  report as it was, or comparing before/after a load.
* **RESTORE** recovers from a bad update or delete in one command, without backups.
* **Atomicity**: a write either commits completely or not at all - the failed batch left no partial rows and
  did not even create a version.
* **Consistency**: schema enforcement and CHECK constraints stop invalid data at write time.
* **Isolation**: readers see a consistent snapshot; concurrent writers do not change what an in-flight read sees.
* **Durability**: every committed version is persisted in the Delta transaction log.

Screenshots (`docs/evidence/day5/`): `01_history_real_table.png`, `02_demo_history.png`, `03_evidence_table.png`,
`04_all_checks_passed.png`.
