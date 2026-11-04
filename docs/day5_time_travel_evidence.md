# Day 5 - Delta Lake ACID and time travel evidence

Notebook: `src/notebooks/05c_delta_acid_time_travel.py` (runs on a DEEP CLONE, `ops.time_travel_demo`, so real
tables are never changed). Every step is asserted and stored in `vstone.ops.time_travel_evidence`:

```sql
SELECT step, property, action, version, rows, value, result
FROM vstone.ops.time_travel_evidence
WHERE run_id = (SELECT MAX(run_id) FROM vstone.ops.time_travel_evidence) ORDER BY step, recorded_at;
```

| Step | Property | What it proves | Result (fill from the query) | Screenshot |
|---|---|---|---|---|
| 1 | History | Every Silver job run is a Delta version (`DESCRIBE HISTORY silver.zip_time_series`) | | `01_history_real_table.png` |
| 2 | Versions | Clone -> bad UPDATE (California x10) -> accidental DELETE (Texas) = 3 versions with operation metrics | | `02_demo_history.png` |
| 3 | Time travel | `VERSION AS OF` and `TIMESTAMP AS OF` return the original row count and California value | | `03_time_travel.png` |
| 4 | Recovery | `RESTORE TABLE ... TO VERSION AS OF` brings rows and values back | | `04_restore.png` |
| 5 | Atomicity | A 5-row batch with 1 row breaking `CHECK (zhvi >= 0)` is rejected as a whole: 0 rows added | | `05_atomicity.png` |
| 6 | Consistency | Schema enforcement rejects a write with an unexpected column | | `06_consistency.png` |
| 7 | Isolation | A reader pinned to a version keeps the same count while rows are deleted | | `07_isolation.png` |
| 8 | Durability | All committed versions stay in `_delta_log`; the first version is still readable | | `08_durability.png` |
