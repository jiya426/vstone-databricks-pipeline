"""Unit tests for the tiered access model generated from config (vstone_framework.security)."""
import pytest

from vstone_framework import security as S
from vstone_framework.config import load_config

SEC = load_config()["security"]
RULES = {k: v for k, v in SEC["mask_rules"].items() if not k.startswith("_")}


# ---------- the mask rules themselves, evaluated in Spark exactly as configured
@pytest.mark.parametrize("case", SEC["self_test"], ids=lambda c: c["rule"])
def test_every_mask_rule_gives_the_expected_output_for_every_level(spark, case):
    rule = RULES[case["rule"]]
    for level, expected in case["expected"].items():
        got = spark.sql(f"SELECT CAST(({rule[level]}) AS STRING) AS out "
                        f"FROM (SELECT CAST({case['input']} AS {rule['type']}) AS value)").first().out
        assert got == expected, (case["rule"], level, got, expected)


def test_precision_gets_coarser_as_the_level_drops(spark):
    usd = RULES["usd"]
    vals = [spark.sql(f"SELECT {usd[l]} AS v FROM (SELECT CAST(532712.4 AS DOUBLE) AS value)").first().v
            for l in ("admin", "analyst", "viewer")]
    assert vals[0] == 532712.4 and vals[1] % 1000 == 0 and vals[2] % 10000 == 0
    assert abs(vals[0] - vals[1]) <= 500 and abs(vals[0] - vals[2]) <= 5000


# ---------- generated SQL
def test_access_level_function_ranks_levels():
    sql = S.access_level_function_sql("vstone.governance.fn_access_level", SEC["groups"], SEC["levels"])
    admin, analyst, viewer = (sql.index(f"THEN '{l}'") for l in ("admin", "analyst", "viewer"))
    assert admin < analyst < viewer                                         # highest level wins
    assert "current_user() = 'vstone_analysts_west'" in sql                 # role sessions recognised
    assert "is_account_group_member('vstone_viewers')" in sql
    assert sql.rstrip().endswith("ELSE 'none' END")


def test_mask_functions_wrap_the_pure_rule():
    sql = S.mask_for_level_sql("vstone.governance.mask_usd_for_level", RULES["usd"])
    assert "FUNCTION vstone.governance.mask_usd_for_level(value DOUBLE, level STRING)" in sql
    assert "WHEN 'viewer' THEN round(value, -4)" in sql
    wrap = S.column_mask_sql("vstone.governance.mask_usd", "vstone.governance.mask_usd_for_level",
                             "vstone.governance.fn_access_level", RULES["usd"])
    assert "RETURN vstone.governance.mask_usd_for_level(value, vstone.governance.fn_access_level())" in wrap


def test_row_filter_by_level_uses_the_parameter():
    sql = S.row_filter_function_sql("g.rf", "g.fn_access_level", "g.state_access_map", SEC["levels"])
    assert "(p_state_code STRING)" in sql and "m.state_code = p_state_code" in sql
    assert "g.fn_access_level() IN ('admin', 'viewer')" in sql
    assert "g.fn_access_level() IN ('analyst')" in sql
    assert "current_user() = m.group_name" in sql


def test_policies_attach_filter_masks_and_sensitivity_tags():
    spec = SEC["secured_tables"]["market_trend_secured"]
    mask_fn = {r: f"g.mask_{r}" for r in RULES}
    out = S.apply_policies_sql("gold.t", "g.rf", "state_code", spec["masks"], mask_fn,
                               {r: v["sensitivity"] for r, v in RULES.items()})
    assert out[0] == "ALTER TABLE gold.t SET ROW FILTER g.rf ON (state_code)"
    assert "ALTER TABLE gold.t ALTER COLUMN zhvi_yoy_pct SET MASK g.mask_pct" in out
    assert "ALTER TABLE gold.t ALTER COLUMN source_file SET TAGS ('sensitivity' = 'internal', 'mask_rule' = 'internal')" in out
    assert len(out) == 1 + 2 * len(spec["masks"])


def test_zip_table_has_no_row_filter_and_generalises_zip():
    spec = SEC["secured_tables"]["zip_home_values_secured"]
    assert spec["row_filter"] is None and spec["masks"]["zip_code"] == "zip"
    sql = S.zip_table_sql("gold.z", "gold.fact", "gold.dim", "zhvi_all_homes", 24, "c", spec["level"])
    assert "r.level = 'zip'" in sql and "CAST(f.metric_value AS DOUBLE) AS home_value_usd" in sql
    assert "-24)" in sql


def test_dynamic_view_hides_and_masks_by_level():
    dv = SEC["dynamic_view"]
    sql = S.dynamic_view_sql("gold.v", "gold.s", ["state_code", "year", "avg_sale_price_usd", "avg_home_value_usd"],
                             dv["filter_column"], "g.rf", "g.lvl", "admin", dv["hidden_columns"],
                             dv["masked_columns"], {"usd": "g.mask_usd_for_level"}, dv["description"])
    assert "CASE WHEN g.lvl() = 'admin' THEN avg_sale_price_usd END AS avg_sale_price_usd" in sql
    assert "g.mask_usd_for_level(CAST(avg_home_value_usd AS DOUBLE), g.lvl()) AS avg_home_value_usd" in sql
    assert sql.rstrip().endswith("WHERE g.rf(s.state_code)")


def test_policy_catalog_documents_every_protected_column():
    rows = S.policy_catalog_rows(SEC, "vstone.gold", "vstone.governance")
    masked = {(r[0].split(".")[-1], r[1]) for r in rows if r[2] == "column mask"}
    for name, spec in SEC["secured_tables"].items():
        assert {(name, c) for c in spec["masks"]} <= masked
    zip_row = next(r for r in rows if r[1] == "zip_code")
    assert zip_row[5:] == ("value", "value", "concat(substr(value, 1, 3), '**')")


def test_grants_are_least_privilege_and_never_for_admins():
    groups = S.non_admin_groups(SEC["groups"])
    assert SEC["admin_group"] not in groups and "vstone_viewers" in groups
    sql = S.grant_sql("vstone", "gold", "governance", ["vstone.gold.market_trend_secured"], ["vstone.governance.f"],
                      "vstone.governance.access_policy_catalog", groups)
    assert "GRANT SELECT ON TABLE vstone.gold.market_trend_secured TO `vstone_viewers`" in sql
    assert "GRANT EXECUTE ON FUNCTION vstone.governance.f TO `vstone_analysts_east`" in sql
    assert not any("ALL PRIVILEGES" in s or "MODIFY" in s for s in sql)


def test_config_is_consistent():
    levels = set(SEC["levels"])
    assert {spec["level"] for spec in SEC["groups"].values()} <= levels
    assert set(SEC["state_access"]) == {g for g, s in SEC["groups"].items() if s["level"] == "analyst"}
    for rule in RULES.values():
        assert {"admin", "analyst", "viewer", "none", "type", "sensitivity"} <= set(rule)
    used = {r for t in SEC["secured_tables"].values() for r in t["masks"].values()}
    assert used <= set(RULES)
