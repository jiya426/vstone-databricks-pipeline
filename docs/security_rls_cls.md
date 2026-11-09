# Security: tiered access model - RLS, column masking, dynamic view, grants (Day 8-9)

**Job:** `vstone_security` (also the last stage of `vstone_master_pipeline`).
**Notebook:** `src/notebooks/08b_security_setup.py`. **Code:** `src/vstone_framework/security.py`.
**Rules:** `conf/vstone_config.json -> security` (nothing is hard-coded in code).

## The model: one decision, enforced everywhere
`governance.fn_access_level()` returns the caller's level. Every row filter and column mask asks it, so changing
who sees what means changing config, not code.

| Level (groups) | Rows | Money (USD) | Growth % | ZIP code | Lineage (file paths, run ids) |
|---|---|---|---|---|---|
| **admin** (`vstone_admins`) | all states | exact | shown | full | shown |
| **analyst** (`vstone_analysts_west`, `vstone_analysts_east`) | only their states | rounded to 1,000 | shown | full | `REDACTED` |
| **viewer** (`vstone_viewers`) | all states | rounded to 10,000 | hidden | `021**` | `REDACTED` |
| **none** (anyone else) | nothing | - | - | - | - |

A person in several groups gets the highest level. Group checks accept account groups
(`is_account_group_member`), workspace-local groups (`is_member`) and **role sessions** (`current_user() = group`:
while acting as a group through the role switcher, the principal is the group itself).

## Techniques
| # | Technique | Object | What it shows |
|---|---|---|---|
| 1 | Policy decision point | `governance.fn_access_level()` | one place decides the level |
| 2 | Row-level security | `governance.rf_state_access(p_state_code)` + `governance.state_access_map` | analysts see only mapped states |
| 3 | Tiered numeric mask | `governance.mask_usd` | precision drops with the level |
| 4 | Suppression mask | `governance.mask_pct` | growth % hidden from viewers |
| 5 | Generalisation mask | `governance.mask_zip` | ZIP code (a quasi-identifier) cut to the 3-digit area |
| 6 | Redaction mask | `governance.mask_internal` | lineage paths and run ids for admins only |
| 7 | Dynamic view | `gold.v_state_sales_by_access` | the same row rule in `WHERE`, a hidden column, masked money |
| 8 | Sensitivity tags | column tags `sensitivity` / `mask_rule` | `confidential`, `restricted`, `quasi_identifier`, `internal` |
| 9 | Policy catalog | `governance.access_policy_catalog` | the access matrix as a table anyone can read |
| 10 | Least privilege | grants | analysts/viewers: SELECT on secured objects only, nothing on raw Gold |
| 11 | Self-test | `ops.sanity_check_results` (layer `security`) | every mask rule x every level checked on each run |

Each mask has a pure form `mask_<rule>_for_level(value, level)` (deterministic, used by the self-test and the
dynamic view) and a column-mask form `mask_<rule>(value)` that applies it for the caller's level.

## Secured objects
| Object | Row filter | Masked columns |
|---|---|---|
| `gold.market_trend_secured` | `state_code` | USD x3 (`usd`), growth x3 (`pct`), `source_file`, `batch_id` (`internal`) |
| `gold.zip_home_values_secured` (last 24 months per ZIP) | - (ZIP has no state) | `zip_code` (`zip`), `home_value_usd` (`usd`), `source_file` (`internal`) |
| `gold.v_state_sales_by_access` (dynamic view) | `WHERE rf_state_access(state_code)` | `avg_sale_price_usd` hidden, 2 USD columns masked |

## One-time setup (Free Edition: no SCIM / account console)
1. **Groups** - Settings -> Identity and access -> Groups -> Add group: `vstone_admins`, `vstone_analysts_west`,
   `vstone_analysts_east`, `vstone_viewers`.
2. **Members** - yourself in `vstone_admins`; the second user (`jiyajain4409@gmail.com`) in `vstone_analysts_west`.
3. **Role switching (optional)** - on `vstone_analysts_west`, `vstone_analysts_east` and `vstone_viewers`:
   Permissions -> Grant access -> yourself -> **Assume**. You can then act as each level from your own login.

## Demo script (run as each level)
Switch role (workspace name, top right) or sign in as the second user; open a **new** query tab, then:
```sql
SELECT current_user() AS principal, vstone.governance.fn_access_level() AS access_level;

SELECT state_code, COUNT(*) AS months, MAX(home_value_usd) AS max_home_value_usd,
       MAX(zhvi_yoy_pct) AS max_yoy_pct, MAX(source_file) AS source_file
FROM vstone.gold.market_trend_secured GROUP BY state_code ORDER BY state_code;

SELECT * FROM vstone.gold.zip_home_values_secured ORDER BY zip_code LIMIT 5;

SELECT * FROM vstone.gold.v_state_sales_by_access ORDER BY state_code, year LIMIT 10;

SELECT * FROM vstone.governance.access_policy_catalog;      -- the matrix, readable by everyone

SELECT * FROM vstone.gold.agg_monthly_market_trend LIMIT 5;  -- non-admins: INSUFFICIENT_PERMISSIONS
```

| Expected | admin | analyst (west) | viewer |
|---|---|---|---|
| `access_level` | admin | analyst | viewer |
| states in the secured table | all | AZ, CA, NV, OR, WA | all |
| CA `max_home_value_usd` | 532,700 | 533,000 | 530,000 |
| `max_yoy_pct` | value | value | NULL |
| `source_file` | path | REDACTED | REDACTED |
| `zip_code` | 01001 | 01001 | 010** |
| raw Gold aggregate | rows | denied | denied |

## Verified on 08-11-2026 (first model)
Second user `jiyajain4409@gmail.com` (west analyst): only AZ, CA, NV, OR, WA; CA 533,000 vs admin 532,700;
sale price NULL in the view; `INSUFFICIENT_PERMISSIONS` on `agg_monthly_market_trend`. Role switching to
`vstone_analysts_west` first returned no rows - fixed by recognising role sessions (`current_user() = group`).
