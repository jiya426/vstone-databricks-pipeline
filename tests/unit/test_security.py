"""Unit tests for the Day 8-9 security SQL generated from config (vstone_framework.security)."""
from vstone_framework import security as S
from vstone_framework.config import load_config

SEC = load_config()["security"]


def test_access_map_rows_come_from_config():
    rows = S.access_map_rows(SEC["state_access"])
    assert ("vstone_analysts_west", "CA") in rows and ("vstone_analysts_east", "NY") in rows
    assert len(rows) == sum(len(v) for v in SEC["state_access"].values())
    assert not any(g == SEC["admin_group"] for g, _ in rows)            # admins need no map: they see all


def test_row_filter_uses_the_parameter_not_the_map_column():
    sql = S.row_filter_function_sql("vstone.governance.rf_state_access", "vstone.governance.state_access_map",
                                    "vstone_admins")
    assert "FUNCTION vstone.governance.rf_state_access(p_state_code STRING)" in sql
    assert "m.state_code = p_state_code" in sql                         # not m.state_code = state_code (always true)
    assert "is_account_group_member('vstone_admins') OR is_member('vstone_admins')" in sql
    assert "RETURNS BOOLEAN" in sql


def test_mask_rounds_for_non_admins():
    sql = S.mask_function_sql("vstone.governance.mask_usd_value", "vstone_admins", -3)
    assert "THEN value ELSE round(value, -3) END" in sql and "RETURNS DOUBLE" in sql


def test_policies_attach_filter_and_one_mask_per_column():
    out = S.apply_policies_sql("vstone.gold.t", "vstone.governance.rf", "state_code", "vstone.governance.m",
                               ["home_value_usd", "monthly_rent_usd"])
    assert out == ["ALTER TABLE vstone.gold.t SET ROW FILTER vstone.governance.rf ON (state_code)",
                   "ALTER TABLE vstone.gold.t ALTER COLUMN home_value_usd SET MASK vstone.governance.m",
                   "ALTER TABLE vstone.gold.t ALTER COLUMN monthly_rent_usd SET MASK vstone.governance.m"]


def test_dynamic_view_filters_rows_and_hides_columns_for_non_admins():
    sql = S.dynamic_view_sql("vstone.gold.v", "vstone.gold.agg_state_sales_summary",
                             ["state_code", "year", "homes_sold", "avg_sale_price_usd"], "state_code",
                             "vstone.governance.state_access_map", "vstone_admins", ["avg_sale_price_usd"])
    assert "CASE WHEN (is_account_group_member('vstone_admins') OR is_member('vstone_admins')) " \
           "THEN avg_sale_price_usd END AS avg_sale_price_usd" in sql
    assert "homes_sold," in sql and "CASE WHEN" not in sql.split("homes_sold")[0].split("AS SELECT")[1]
    assert "m.state_code = s.state_code" in sql


def test_grants_are_least_privilege_and_only_for_analysts():
    groups = S.analyst_groups(SEC["groups"], SEC["admin_group"])
    assert SEC["admin_group"] not in groups and len(groups) == 2
    sql = S.grant_sql("vstone", ["gold"], ["vstone.gold.market_trend_secured"], groups)
    assert "GRANT USE CATALOG ON CATALOG vstone TO `vstone_analysts_west`" in sql
    assert "GRANT SELECT ON TABLE vstone.gold.market_trend_secured TO `vstone_analysts_east`" in sql
    assert not any("ALL PRIVILEGES" in s or "MODIFY" in s for s in sql)


def test_every_mapped_group_is_a_configured_group():
    assert set(SEC["state_access"]) <= set(SEC["groups"])
