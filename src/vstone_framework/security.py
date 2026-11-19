"""Day 8-9 governance: a tiered access model generated from conf/vstone_config.json -> security.

    one policy decision point   governance.fn_access_level()      -> 'admin' | 'analyst' | 'viewer' | 'none'
    pure mask rules             governance.mask_<rule>_for_level(value, level)    (deterministic, self-tested)
    column masks                governance.mask_<rule>(value) = mask_<rule>_for_level(value, fn_access_level())
    row filter                  governance.rf_state_access(p_state_code)  - all / mapped states / nothing by level
    access map                  governance.state_access_map       (analyst group -> states)
    policy catalog              governance.access_policy_catalog  (who sees what, per object and column)
    secured objects             gold.market_trend_secured, gold.zip_home_values_secured, gold.v_state_sales_by_access

Group checks accept three cases, so the model works for every way a person can reach Databricks:
  is_account_group_member(g) - member of account group g
  is_member(g)               - member of workspace-local group g
  current_user() = g         - session ACTING AS group g (RBAC role switcher: the principal is the group itself)
"""
from __future__ import annotations

from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from .metadata import sql_string

LEVEL_ORDER = ("admin", "analyst", "viewer", "none")


def in_group(group_expr: str) -> str:
    return (f"(is_account_group_member({group_expr}) OR is_member({group_expr}) "
            f"OR current_user() = {group_expr})")


def groups_by_level(groups: Mapping[str, Mapping]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {lvl: [] for lvl in LEVEL_ORDER}
    for g, spec in groups.items():
        out[spec["level"]].append(g)
    return out


# ------------------------------------------------------------------ policy decision point
def access_level_function_sql(fn: str, groups: Mapping[str, Mapping], levels: Mapping[str, Mapping]) -> str:
    """The caller's level: the highest-ranked level among the groups they belong to (or act as)."""
    by_level = groups_by_level(groups)
    ordered = sorted((l for l in levels if l != "none" and by_level.get(l)), key=lambda l: levels[l]["rank"])
    whens = "\n".join(f"  WHEN {' OR '.join(in_group(sql_string(g)) for g in by_level[l])} THEN '{l}'" for l in ordered)
    return f"""CREATE OR REPLACE FUNCTION {fn}()
RETURNS STRING
COMMENT {sql_string('Policy decision point: access level of the caller (' + ', '.join(ordered + ['none']) + ')')}
RETURN CASE
{whens}
  ELSE 'none' END"""


# ------------------------------------------------------------------ masks
def mask_for_level_sql(fn: str, rule: Mapping) -> str:
    """Pure, deterministic mask: (value, level) -> what that level sees. Self-tested in Databricks."""
    t = rule["type"]
    whens = "\n".join(f"  WHEN '{lvl}' THEN {rule[lvl]}" for lvl in LEVEL_ORDER if lvl != "none")
    return f"""CREATE OR REPLACE FUNCTION {fn}(value {t}, level STRING)
RETURNS {t}
COMMENT {sql_string(rule['description'])}
RETURN CASE level
{whens}
  ELSE {rule['none']} END"""


def column_mask_sql(fn: str, for_level_fn: str, access_fn: str, rule: Mapping) -> str:
    """Column mask attached to columns: applies the pure rule for the caller's level."""
    t = rule["type"]
    return f"""CREATE OR REPLACE FUNCTION {fn}(value {t})
RETURNS {t}
COMMENT {sql_string('Column mask: ' + rule['description'])}
RETURN {for_level_fn}(value, {access_fn}())"""


def mask_names(schema: str, prefix: str, rule_name: str) -> Tuple[str, str]:
    """(column-mask function, pure for-level function) for a rule."""
    return f"{schema}.{prefix}{rule_name}", f"{schema}.{prefix}{rule_name}_for_level"


# ------------------------------------------------------------------ rows
def access_map_rows(state_access: Mapping[str, Sequence[str]]) -> List[Tuple[str, str]]:
    return sorted((g, s) for g, states in state_access.items() for s in states)


def access_table_ddl(table: str) -> str:
    return (f"CREATE TABLE IF NOT EXISTS {table} (group_name STRING NOT NULL COMMENT 'Workspace group', "
            f"state_code STRING NOT NULL COMMENT '2-letter state code the group may see') "
            f"COMMENT 'Row-level security access map: which analyst group may see which states'")


def row_filter_function_sql(fn: str, access_fn: str, access_table: str, levels: Mapping[str, Mapping]) -> str:
    """Row filter by level: 'all' -> every row, 'mapped_states' -> states mapped to one of the caller's groups.
    The parameter is p_state_code on purpose: an unqualified `state_code` inside the subquery would bind to the map's
    column (m.state_code = m.state_code is always TRUE) and silently disable the filter."""
    all_levels = [l for l, s in levels.items() if s["row_scope"] == "all"]
    mapped = [l for l, s in levels.items() if s["row_scope"] == "mapped_states"]
    level_list = ", ".join(f"'{l}'" for l in all_levels)
    mapped_list = ", ".join(f"'{l}'" for l in mapped)
    return f"""CREATE OR REPLACE FUNCTION {fn}(p_state_code STRING)
RETURNS BOOLEAN
COMMENT {sql_string('Row filter: levels ' + '/'.join(all_levels) + ' see every row; ' + '/'.join(mapped) + ' only states mapped to their group in ' + access_table + '; everyone else nothing')}
RETURN {access_fn}() IN ({level_list})
    OR ({access_fn}() IN ({mapped_list})
        AND EXISTS (SELECT 1 FROM {access_table} m
                    WHERE m.state_code = p_state_code AND {in_group('m.group_name')}))"""


# ------------------------------------------------------------------ secured objects
def zip_table_sql(table: str, fact: str, regions: str, metric_key: str, months: int, comment: str,
                  level: str) -> str:
    """ZIP-level home values for the last N months (money as DOUBLE so the USD mask type matches)."""
    return f"""CREATE OR REPLACE TABLE {table}
COMMENT {sql_string(comment)}
AS SELECT r.region_code AS zip_code,
          to_date(CAST(f.date_key AS STRING), 'yyyyMMdd') AS date,
          CAST(f.metric_value AS DOUBLE) AS home_value_usd,
          f.source_file
FROM {fact} f JOIN {regions} r ON r.region_key = f.region_key AND r.level = '{level}'
WHERE f.metric_key = '{metric_key}'
  AND to_date(CAST(f.date_key AS STRING), 'yyyyMMdd') >
      add_months((SELECT to_date(CAST(MAX(date_key) AS STRING), 'yyyyMMdd') FROM {fact}
                  WHERE metric_key = '{metric_key}'), -{int(months)})"""


def copy_table_sql(table: str, source: str, comment: str) -> str:
    return f"CREATE OR REPLACE TABLE {table} COMMENT {sql_string(comment)} AS SELECT * FROM {source}"


def apply_policies_sql(table: str, row_filter_fn: Optional[str], filter_column: Optional[str],
                       masks: Mapping[str, str], mask_fn_of: Mapping[str, str],
                       sensitivity_of: Mapping[str, str]) -> List[str]:
    """ALTER statements: row filter, one mask per protected column, and a sensitivity tag per protected column."""
    out = [f"ALTER TABLE {table} SET ROW FILTER {row_filter_fn} ON ({filter_column})"] if row_filter_fn else []
    for col, rule in masks.items():
        out.append(f"ALTER TABLE {table} ALTER COLUMN {col} SET MASK {mask_fn_of[rule]}")
        out.append(f"ALTER TABLE {table} ALTER COLUMN {col} SET TAGS ('sensitivity' = '{sensitivity_of[rule]}', "
                   f"'mask_rule' = '{rule}')")
    return out


def dynamic_view_sql(view: str, source: str, columns: Sequence[str], filter_column: str, row_filter_fn: str,
                     access_fn: str, admin_level: str, hidden_columns: Sequence[str],
                     masked_columns: Mapping[str, str], for_level_fn_of: Mapping[str, str], comment: str) -> str:
    """Dynamic view: the same row rule in WHERE, hidden columns NULL for non-admins, masked columns by level."""
    def expr(c: str) -> str:
        if c in hidden_columns:
            return f"CASE WHEN {access_fn}() = '{admin_level}' THEN {c} END AS {c}"
        if c in masked_columns:
            return f"{for_level_fn_of[masked_columns[c]]}(CAST({c} AS DOUBLE), {access_fn}()) AS {c}"
        return c
    select = ",\n       ".join(expr(c) for c in columns)
    return f"""CREATE OR REPLACE VIEW {view}
COMMENT {sql_string(comment)}
AS SELECT {select}
FROM {source} s
WHERE {row_filter_fn}(s.{filter_column})"""


# ------------------------------------------------------------------ catalog, grants, self-test
def policy_catalog_rows(sec: Mapping, gold: str, governance: str) -> List[Tuple]:
    """(object, column, technique, rule, sensitivity, admin, analyst, viewer) - the access matrix, from config."""
    rules, levels = sec["mask_rules"], sec["levels"]
    rows: List[Tuple] = []
    for name, spec in sec["secured_tables"].items():
        obj = f"{gold}.{name}"
        if spec.get("row_filter"):
            rows.append((obj, spec["row_filter"], "row filter", "rf_state_access", None,
                         *[levels[l]["row_scope"] for l in ("admin", "analyst", "viewer")]))
        for col, rule in spec["masks"].items():
            rows.append((obj, col, "column mask", rule, rules[rule]["sensitivity"],
                         *[rules[rule][l] for l in ("admin", "analyst", "viewer")]))
    dv = sec["dynamic_view"]
    obj = f"{gold}.{dv['name']}"
    rows.append((obj, dv["filter_column"], "dynamic view (WHERE)", "rf_state_access", None,
                 *[levels[l]["row_scope"] for l in ("admin", "analyst", "viewer")]))
    rows += [(obj, c, "dynamic view (hidden)", "admin only", "confidential", "value", "NULL", "NULL")
             for c in dv["hidden_columns"]]
    rows += [(obj, c, "dynamic view (mask)", r, rules[r]["sensitivity"], *[rules[r][l] for l in ("admin", "analyst", "viewer")])
             for c, r in dv["masked_columns"].items()]
    rows.append((f"{governance}.{sec['access_table']}", "group_name, state_code", "access map", None, None,
                 "read", "no access", "no access"))
    return rows


POLICY_CATALOG_SCHEMA = ("object_name STRING, column_name STRING, technique STRING, rule STRING, sensitivity STRING, "
                         "admin_sees STRING, analyst_sees STRING, viewer_sees STRING")


def self_test_sql(for_level_fn: str, input_literal: str, level: str) -> str:
    return f"SELECT CAST({for_level_fn}({input_literal}, '{level}') AS STRING) AS out"


def grant_sql(catalog: str, gold_schema: str, governance_schema: str, objects: Sequence[str],
              functions: Sequence[str], catalog_table: str, groups: Sequence[str]) -> List[str]:
    """Least privilege: browse the catalog, SELECT on the secured objects and the policy catalog,
    EXECUTE on the functions the dynamic view calls. Nothing on the unsecured Gold tables."""
    out: List[str] = []
    for g in groups:
        q = f"`{g}`"
        out.append(f"GRANT USE CATALOG ON CATALOG {catalog} TO {q}")
        out += [f"GRANT USE SCHEMA ON SCHEMA {catalog}.{s} TO {q}" for s in (gold_schema, governance_schema)]
        out += [f"GRANT SELECT ON TABLE {o} TO {q}" for o in list(objects) + [catalog_table]]
        out += [f"GRANT EXECUTE ON FUNCTION {f} TO {q}" for f in functions]
    return out


def non_admin_groups(groups: Mapping[str, Mapping]) -> List[str]:
    return [g for g, spec in groups.items() if spec["level"] != "admin"]
