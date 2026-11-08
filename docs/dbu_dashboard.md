# DBU resource-usage dashboard (Day 8-9)

An AI/BI dashboard on the billing system tables shows where compute (DBUs) goes. Creating a dashboard is a
one-time UI task (it is not a job or pipeline run).

## Create it
1. Sidebar -> **Dashboards** -> **Create dashboard**. Name: `Vstone - DBU usage`.
2. **Data** tab -> **Create from SQL** -> paste each query below as its own dataset (name in the heading).
3. **Canvas** tab -> add a visualization per dataset (type in brackets), then **Publish**.
4. Screenshot the published dashboard. Optional: **File -> Export** the `.lvdash.json` into `resources/` so the
   dashboard is deployed by the bundle too.

Free Edition does not bill, but usage is still metered; `list_cost_usd` shows what the same work would cost
at list price.

### 1. `dbu_by_day_product` (stacked bar: x = usage_date, y = dbus, colour = product)
```sql
SELECT usage_date, billing_origin_product AS product, SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_unit = 'DBU' AND usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL ORDER BY usage_date
```

### 2. `dbu_by_job` (bar: x = dbus, y = job_name)
```sql
WITH jobs AS (
  SELECT workspace_id, job_id, name,
         ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
  FROM system.lakeflow.jobs)
SELECT COALESCE(j.name, u.usage_metadata.job_id) AS job_name, SUM(u.usage_quantity) AS dbus
FROM system.billing.usage u
LEFT JOIN jobs j ON j.rn = 1 AND j.workspace_id = u.workspace_id AND j.job_id = u.usage_metadata.job_id
WHERE u.usage_metadata.job_id IS NOT NULL AND u.usage_unit = 'DBU'
GROUP BY ALL ORDER BY dbus DESC
```

### 3. `dbu_by_pipeline` (bar: DLT pipelines)
```sql
SELECT u.usage_metadata.dlt_pipeline_id AS pipeline_id, SUM(u.usage_quantity) AS dbus
FROM system.billing.usage u
WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL AND u.usage_unit = 'DBU'
GROUP BY ALL ORDER BY dbus DESC
```

### 4. `list_cost_by_day` (line: x = usage_date, y = list_cost_usd) and a counter on its total
```sql
SELECT u.usage_date, ROUND(SUM(u.usage_quantity * lp.pricing.default), 2) AS list_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON lp.sku_name = u.sku_name AND lp.usage_unit = u.usage_unit
 AND u.usage_end_time >= lp.price_start_time
 AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL ORDER BY u.usage_date
```

### 5. `job_run_durations` (table) - run time per job run
```sql
SELECT j.name AS job_name, t.run_id, MIN(t.period_start_time) AS started,
       ROUND(SUM(unix_timestamp(t.period_end_time) - unix_timestamp(t.period_start_time)) / 60, 1) AS minutes,
       MAX(t.result_state) AS result
FROM system.lakeflow.job_run_timeline t
LEFT JOIN (SELECT workspace_id, job_id, name,
                  ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
           FROM system.lakeflow.jobs) j
  ON j.rn = 1 AND j.workspace_id = t.workspace_id AND j.job_id = t.job_id
WHERE t.period_start_time >= current_date() - INTERVAL 14 DAYS
GROUP BY ALL ORDER BY started DESC
```

If a billing query returns no rows, wait a few hours: usage records arrive with a delay.
