# Performance benchmark: Liquid Clustering vs partitioning + Z-order (Day 7)

**Job:** `vstone_performance_benchmark` (GitHub Actions -> Run workflow -> `performance_benchmark`).
**Notebook:** `src/notebooks/07a_performance_benchmark.py`. **Results table:** `vstone.ops.performance_benchmark`.

## Method
The Gold fact `fact_housing_metric_monthly` (one row per region x metric x month) is copied into 3 tables that
differ only in layout. Each copy is optimised once, then the same 5 queries run `benchmark_runs` (3) times.

| Layout | Table | DDL | Maintenance |
|---|---|---|---|
| No layout (baseline) | `ops.bench_fact_plain` | - | `OPTIMIZE` (compaction only) |
| Partitioning + Z-order | `ops.bench_fact_partition_zorder` | `PARTITIONED BY (metric_key)` | `OPTIMIZE ... ZORDER BY (region_key, date_key)` |
| Liquid Clustering | `ops.bench_fact_liquid` | `CLUSTER BY (metric_key, region_key, date_key)` | `OPTIMIZE` (incremental clustering) |

| Query | Pattern |
|---|---|
| Q1 | One region, one metric, all months (point lookup) |
| Q2 | One metric, one month, all regions |
| Q3 | One metric, 3-year range, yearly average |
| Q4 | All metrics of the state level for one month (prefix filter on region_key) |
| Q5 | Full-scan aggregate (no filter - baseline) |

Two measures per query and layout: **median seconds** and **files with matching rows** - the number of files
that contain at least one selected row, i.e. the minimum a reader must open after data skipping.

## Results
_Fill in from the comparison table at the end of the notebook run (screenshot also in `docs/evidence/day7/`)._

| Query | No layout (s) | Partition + Z-order (s) | Liquid (s) | No layout files | Partition + Z-order files | Liquid files |
|---|---|---|---|---|---|---|
| Q1 | | | | | | |
| Q2 | | | | | | |
| Q3 | | | | | | |
| Q4 | | | | | | |
| Q5 | | | | | | |

Table sizes and file counts (from `DESCRIBE DETAIL`, first result of the notebook): _fill in_.

## Conclusion
_Write after the run._ Points to cover: which layout touched the fewest files for Q1-Q4; whether partitioning by
`metric_key` created many small files; that Liquid Clustering needs no partition design up front and can change its
keys later (`ALTER TABLE ... CLUSTER BY`) without rewriting the table - which is why the Gold fact uses it.

Note: serverless compute caches data between runs, so the median of repeated runs mostly reflects warm reads;
the file counts are the layout-independent evidence of data skipping.
