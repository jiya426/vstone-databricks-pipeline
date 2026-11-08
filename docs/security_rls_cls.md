# Security: groups, row-level security, column masking and a dynamic view (Day 8-9)

**Job:** `vstone_security` (also the last stage of `vstone_master_pipeline`).
**Notebook:** `src/notebooks/08b_security_setup.py`. **Rules:** `conf/vstone_config.json -> security`.

## Who sees what
| Group | Rows of `gold.market_trend_secured` | USD columns | `gold.v_state_sales_by_access` |
|---|---|---|---|
| `vstone_admins` | all states | exact | all states, all columns |
| `vstone_analysts_west` | CA, OR, WA, NV, AZ | rounded to 1,000 | their states, `avg_sale_price_usd` hidden |
| `vstone_analysts_east` | NY, NJ, MA, PA, FL | rounded to 1,000 | their states, `avg_sale_price_usd` hidden |
| anyone else | no access (no grant) | - | - |

## Objects
| Technique | Object | How |
|---|---|---|
| Access map | `governance.state_access_map` | group -> state rows, written from config |
| Row-level security | `governance.rf_state_access(p_state_code)` | `ALTER TABLE ... SET ROW FILTER ... ON (state_code)` |
| Column-level security | `governance.mask_usd_value(value)` | `ALTER TABLE ... ALTER COLUMN ... SET MASK` on 3 USD columns |
| Dynamic view | `gold.v_state_sales_by_access` | same row rule in `WHERE`, `CASE WHEN <admin> THEN col END` for the hidden column |
| Least privilege | grants | analysts: `USE CATALOG`, `USE SCHEMA gold`, `SELECT` on the 2 secured objects only |

Group checks use `is_account_group_member()` OR `is_member()`, so they work for account groups and
workspace-local groups.

## One-time setup (Free Edition has no SCIM / account console)
1. **Invite a second user** - Settings -> Workspace admin -> Identity and access -> Users -> Manage -> Add user ->
   invite a second e-mail address you own. Accept the invite from that mailbox.
2. **Create the 3 groups** - Settings -> Identity and access -> Groups -> Add group:
   `vstone_admins`, `vstone_analysts_west`, `vstone_analysts_east`.
3. **Add members** - yourself to `vstone_admins`; the second user to `vstone_analysts_west`.
4. Run **Actions -> Run workflow -> security** (or the master pipeline).

## Demo (evidence)
As **yourself** (admin) and then as the **second user** (incognito window), run:
```sql
SELECT state_code, COUNT(*) AS months, MAX(home_value_usd) AS max_home_value_usd
FROM vstone.gold.market_trend_secured GROUP BY state_code ORDER BY state_code;

SELECT * FROM vstone.gold.v_state_sales_by_access ORDER BY state_code, year LIMIT 20;

SELECT * FROM vstone.gold.agg_monthly_market_trend LIMIT 5;   -- second user: PERMISSION_DENIED
```
| Expected | Admin | Second user (west) |
|---|---|---|
| States in the secured table | all ~50 | only CA, OR, WA, NV, AZ |
| `max_home_value_usd` | exact, e.g. 512345.67 | rounded, e.g. 512000 |
| `avg_sale_price_usd` in the view | values | NULL |
| Unsecured Gold aggregate | rows | permission denied |

Screenshot the three queries for both users.
