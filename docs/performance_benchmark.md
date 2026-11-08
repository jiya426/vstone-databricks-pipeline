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

## Results (run 08-11-2026, job run 232452812302709, 3 runs per query)

### The 3 copies
| Layout | Build + OPTIMIZE (s) | Files | Size | Layout columns |
|---|---|---|---|---|
| No layout | 47.7 | 9 | 724 MB | - |
| Partition + Z-order | 40.7 | 96 | 578 MB | partitioned by `metric_key`, Z-order `region_key, date_key` |
| Liquid Clustering | 55.3 | 14 | 733 MB | `metric_key, region_key, date_key` |

### Files with matching rows (data skipping) - lower is better
| Query | Pattern | No layout (of 9) | Partition + Z-order (of 96) | Liquid (of 14) |
|---|---|---|---|---|
| Q1 | one region, one metric, all months | 2 | **1** | 2 |
| Q2 | one metric, one month | 3 | **1** | 2 |
| Q3 | one metric, 3-year range | 2 | **1** | 3 |
| Q4 | state level, one month, all metrics (no metric filter) | **3** | 77 | **3** |
| Q5 | full scan | 9 | 96 | 14 |

### Median / first-run time (seconds)
| Query | No layout | Partition + Z-order | Liquid |
|---|---|---|---|
| Q1 | 0.90 / 2.32 | 0.69 / 1.54 | 0.73 / 1.45 |
| Q2 | 1.08 / 1.90 | 1.00 / 1.10 | 0.93 / 1.66 |
| Q3 | 1.22 / 2.04 | 0.77 / 0.77 | 0.99 / 1.54 |
| Q4 | 1.06 / 1.72 | 1.05 / 1.77 | 0.98 / 1.47 |
| Q5 | 1.40 / 1.53 | 0.61 / 0.70 | 1.47 / 1.69 |

## Findings
1. **At 0.7 GB every layout answers in about 1 second.** Serverless caches data after the first read, so medians
   differ by fractions of a second; the file counts are the clearer evidence.
2. **Partitioning wins only when the query filters the partition column.** Q1-Q3 filter `metric_key` and read
   1 file. Q4 does not, and has to open 77 of 96 files - 25x more than Liquid Clustering (3 of 14).
3. **Partitioning by `metric_key` creates 96 small files (about 6 MB each)** - the small-file problem.
   Databricks' guidance is to avoid partitioning tables under about 1 TB.
4. **Liquid Clustering is consistently good on every pattern** (2-3 files for Q1-Q4) without choosing a partition
   column up front, and its keys can change later (`ALTER TABLE ... CLUSTER BY`) without rewriting the table.
5. **Q5 (full scan) is fastest on the partitioned table** because the count per `metric_key` can be answered
   per partition; a full scan is not a pattern we optimise for.
6. **Caveat:** the "no layout" copy was created from the Gold fact, which is already clustered on the same keys,
   so it inherited that row order and its file counts look better than a truly random table would.

## Conclusion
**Liquid Clustering is the right layout for the Gold fact** (`CLUSTER BY metric_key, region_key, date_key`):
near-best data skipping for every query pattern, no small-file problem, no partition design to get wrong and
keys that can evolve. Partitioning + Z-order is only better for queries that always filter the partition
column, and costs 25x more files for those that do not.
