"""Unit tests for Day 7: churn-style metric, latest complete month, late-arriving batch and MERGE."""
import datetime as dt
import os
from decimal import Decimal

from pyspark.sql import functions as F

from vstone_framework.chunk_io import LocalFs, write_single_file
from vstone_framework.gold import churn_checks, churn_summary, latest_complete_month, market_churn
from vstone_framework.gold_schema import AGG_CHURN, AGG_CHURN_SUMMARY, column_names
from vstone_framework.incremental import late_batch_frame, merge_metrics, merge_sql, value_columns

D = dt.date.fromisoformat


def regions(spark):
    return spark.createDataFrame(
        [(f"zip:{z}", "zip", z, "regional", "MA") for z in ("01001", "01002", "01003", "01004")] +
        [("state:UnitedStates", "state", "United States", "national", None)],
        "region_key string, level string, region_name string, region_type string, state_code string")


def month_keys(start, n):
    y, m = start
    out = []
    for _ in range(n):
        d = dt.date(y + (m // 12), m % 12 + 1, 1) - dt.timedelta(days=1)
        out.append(int(d.strftime("%Y%m%d")))
        m += 1
    return out


def fact(spark, series):
    rows = [(rk, "zhvi_all_homes", k, Decimal(v)) for rk, (keys, values) in series.items() for k, v in zip(keys, values)]
    return spark.createDataFrame(rows, "region_key string, metric_key string, date_key int, metric_value decimal(18,4)")


def test_churn_statuses(spark):
    m12 = month_keys((2017, 1), 12)                                   # Jan..Dec 2017
    f = fact(spark, {
        "zip:01001": (m12, [100 + i for i in range(12)]),            # rising -> active
        "zip:01002": (m12, [200 - i for i in range(12)]),            # 11 falls in a row -> cooling
        "zip:01003": (m12[:5], [300] * 5),                           # stopped in May -> inactive (7 months)
        "zip:01004": (m12, [100] * 6 + [99, 98, 97, 96, 95, 96]),    # fell 5 times, then rose -> active
    })
    out = market_churn(f, regions(spark), "zhvi_all_homes", 6, 6, "fact", "b")
    assert out.columns == column_names(AGG_CHURN)
    got = {r.region_key: (r.status, r.consecutive_monthly_declines, r.months_since_last_data) for r in out.collect()}
    assert got == {"zip:01001": ("active", 0, 0), "zip:01002": ("cooling", 11, 0),
                   "zip:01003": ("inactive", 0, 7), "zip:01004": ("active", 0, 0)}


def test_a_gap_breaks_a_decline_streak(spark):
    keys = month_keys((2017, 1), 12)
    keys = keys[:3] + keys[4:]                                        # April missing
    f = fact(spark, {"zip:01001": (keys, [200 - i for i in range(11)])})
    r = market_churn(f, regions(spark), "zhvi_all_homes", 6, 6, "fact", "b").first()
    assert r.consecutive_monthly_declines == 7 and r.status == "cooling"   # counted only after the gap


def test_churn_summary_and_checks(spark):
    m12 = month_keys((2017, 1), 12)
    f = fact(spark, {"zip:01001": (m12, [100 + i for i in range(12)]), "zip:01002": (m12, [200 - i for i in range(12)])})
    churn = market_churn(f, regions(spark), "zhvi_all_homes", 6, 6, "fact", "b")
    summary = churn_summary(churn, "UNKNOWN", "churn", "b")
    assert summary.columns == column_names(AGG_CHURN_SUMMARY)
    s = summary.first()
    assert (s.regions, s.active_regions, s.cooling_regions, s.churn_rate_pct) == (2, 1, 1, 50.0)
    assert [r for r in churn_checks(churn, summary, ["active", "cooling", "inactive"], "churn") if r.status == "FAIL"] == []


def test_latest_complete_month_ignores_a_few_early_rows(spark):
    rows = [("zip", D("2017-12-31"), f"zip:{i}") for i in range(10)] + [("zip", D("2018-01-31"), "zip:0")]
    df = spark.createDataFrame(rows, "level string, date date, region_key string")
    assert latest_complete_month(df, 0.5).first().date == D("2017-12-31")
    assert latest_complete_month(df, 0.0).first().date == D("2018-01-31")


def test_late_batch_revises_latest_month_and_adds_next_month(spark):
    cols = ["Date", "RegionName", "RegionLevel", "ZHVI_AllHomes", "ZRI_AllHomes"]
    src = spark.createDataFrame([("2017-11-30", "01001", "zip", "190000", "1500"),
                                 ("2017-12-31", "01001", "zip", "200000", "1600"),
                                 ("2017-12-31", "01002", "zip", "300000", None),
                                 ("2017-12-31", "01003", "zip", None, "1700"),          # no ZHVI -> not picked
                                 ("2017-12-31", "Texas", "state", "250000", None)],
                                ", ".join(f"{c} string" for c in cols))
    out = {(r.Date, r.RegionName): r for r in
           late_batch_frame(src, cols, "RegionLevel", "zip", "ZHVI_AllHomes", 10, 1.0, 0.5).collect()}
    assert set(out) == {("2017-12-31", "01001"), ("2017-12-31", "01002"), ("2018-01-31", "01001"), ("2018-01-31", "01002")}
    assert out[("2017-12-31", "01001")].ZHVI_AllHomes == "202000"                 # +1 % revision
    assert out[("2017-12-31", "01001")].ZRI_AllHomes == "1600"                    # other metrics unchanged
    assert out[("2018-01-31", "01001")].ZHVI_AllHomes == "203010"                 # +0.5 % next month
    assert out[("2018-01-31", "01001")].ZRI_AllHomes is None
    assert out[("2018-01-31", "01001")].RegionLevel == "zip"


def test_late_batch_limits_region_count(spark):
    cols = ["Date", "RegionName", "RegionLevel", "ZHVI_AllHomes"]
    src = spark.createDataFrame([("2017-12-31", f"0100{i}", "zip", "1") for i in range(5)],
                                ", ".join(f"{c} string" for c in cols))
    assert late_batch_frame(src, cols, "RegionLevel", "zip", "ZHVI_AllHomes", 2, 1.0, 0.5).count() == 4


def test_merge_sql_is_an_idempotent_upsert():
    sql = merge_sql("vstone.silver.zip_time_series", "late_updates", ["date", "region_name"],
                    value_columns(["date", "region_name", "zhvi_all_homes", "load_dt", "batch_id"],
                                  ["load_dt", "source", "source_file", "batch_id"]))
    assert "ON t.`date` = s.`date` AND t.`region_name` = s.`region_name`" in sql
    assert "WHEN MATCHED AND (NOT (t.`zhvi_all_homes` <=> s.`zhvi_all_homes`)) THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql
    assert "load_dt" not in sql                                       # audit columns never trigger an update


def test_merge_metrics_from_history_row():
    row = {"operation": "MERGE", "version": 7,
           "operationMetrics": {"numTargetRowsUpdated": "100", "numTargetRowsInserted": "100", "numSourceRows": "200"}}
    assert merge_metrics(row) == {"operation": "MERGE", "version": 7, "updated": 100, "inserted": 100, "source_rows": 200}


def test_late_batches_keep_earlier_files(spark, tmp_path):
    folder = tmp_path / "late_arriving"
    for batch in ("batch1", "batch2"):
        df = spark.createDataFrame([(batch,)], "a string")
        write_single_file(df, "csv", str(folder / "_staging" / batch), str(folder / f"late_{batch}.csv"), LocalFs(),
                          clear_folder=False)
    assert sorted(os.listdir(folder)) == ["late_batch1.csv", "late_batch2.csv"]
