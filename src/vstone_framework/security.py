"""Day 8-9 governance: SQL for row-level security, column masking, a dynamic view and grants.

Everything is generated from conf/vstone_config.json -> security, so the rules live in one place:

  governance.state_access_map      group -> state_code it may see          (access map)
  governance.rf_state_access()     row filter: admins see all, others only mapped states      (RLS)
  governance.mask_usd_value()      column mask: admins see exact USD, others rounded           (CLS)
  gold.market_trend_secured        copy of the trend aggregate with the row filter + masks applied
  gold.v_state_sales_by_access     dynamic view: same row rule + a hidden column, written in SQL

Group checks use both is_account_group_member() and is_member(): groups created in a workspace's settings
can be account groups or workspace-local groups depending on the workspace setup.
"""
from __future__ import annotations

from typing import Dict, List, Mapping, Sequence, Tuple

from .metadata import sql_string


def in_group(group_expr: str) -> str:
    return f"(is_account_group_member({group_expr}) OR is_member({group_expr}))"


def access_map_rows(state_access: Mapping[str, Sequence[str]]) -> List[Tuple[str, str]]:
    """(group_name, state_code) rows of the access map, sorted for stable output."""
    return sorted((g, s) for g, states in state_access.items() for s in states)


def access_table_ddl(table: str) -> str:
    return (f"CREATE TABLE IF NOT EXISTS {table} (group_name STRING NOT NULL COMMENT 'Workspace group', "
            f"state_code STRING NOT NULL COMMENT '2-letter state code the group may see') "
            f"COMMENT 'Row-level security access map: which group may see which states'")


def row_filter_function_sql(fn: str, access_table: str, admin_group: str) -> str:
    """Row filter: TRUE for the admin group; otherwise TRUE only for states mapped to one of the user's groups."""
    # The parameter is named p_state_code on purpose: inside the subquery an unqualified `state_code` would bind to
    # the access-map column (m.state_code = m.state_code is always TRUE) and silently disable the filter.
    return f"""CREATE OR REPLACE FUNCTION {fn}(p_state_code STRING)
RETURNS BOOLEAN
COMMENT {sql_string(f'Row filter: {admin_group} sees every row; other groups only the states mapped to them in {access_table}')}
RETURN {in_group(sql_string(admin_group))}
    OR EXISTS (SELECT 1 FROM {access_table} m
               WHERE m.state_code = p_state_code AND {in_group('m.group_name')})"""


def mask_function_sql(fn: str, admin_group: str, round_digits: int) -> str:
    """Column mask: exact value for the admin group, rounded value for everyone else."""
    return f"""CREATE OR REPLACE FUNCTION {fn}(value DOUBLE)
RETURNS DOUBLE
COMMENT {sql_string(f'Column mask: exact USD for {admin_group}; rounded to 10^{-round_digits} for other users')}
RETURN CASE WHEN {in_group(sql_string(admin_group))} THEN value ELSE round(value, {round_digits}) END"""


def secured_table_sql(table: str, source: str, comment: str) -> str:
    return f"CREATE OR REPLACE TABLE {table} COMMENT {sql_string(comment)} AS SELECT * FROM {source}"


def apply_policies_sql(table: str, row_filter_fn: str, filter_column: str, mask_fn: str,
                       masked_columns: Sequence[str]) -> List[str]:
    """ALTER statements attaching the row filter and one mask per protected column."""
    out = [f"ALTER TABLE {table} SET ROW FILTER {row_filter_fn} ON ({filter_column})"]
    out += [f"ALTER TABLE {table} ALTER COLUMN {c} SET MASK {mask_fn}" for c in masked_columns]
    return out


def dynamic_view_sql(view: str, source: str, columns: Sequence[str], filter_column: str, access_table: str,
                     admin_group: str, hidden_columns: Sequence[str]) -> str:
    """Dynamic view: the same row rule written as a WHERE clause, and hidden columns NULL for non-admins."""
    admin = in_group(sql_string(admin_group))
    select = ",\n       ".join(f"CASE WHEN {admin} THEN {c} END AS {c}" if c in hidden_columns else c
                                for c in columns)
    return f"""CREATE OR REPLACE VIEW {view}
COMMENT {sql_string(f'Dynamic view of {source}: {admin_group} sees all states and columns; other groups only their mapped states, with {", ".join(hidden_columns)} hidden')}
AS SELECT {select}
FROM {source} s
WHERE {admin}
   OR EXISTS (SELECT 1 FROM {access_table} m
              WHERE m.state_code = s.{filter_column} AND {in_group('m.group_name')})"""


def grant_sql(catalog: str, schemas: Sequence[str], objects: Sequence[str], groups: Sequence[str]) -> List[str]:
    """Least privilege for analyst groups: browse the catalog/schemas, SELECT only the secured objects."""
    out: List[str] = []
    for g in groups:
        q = f"`{g}`"
        out.append(f"GRANT USE CATALOG ON CATALOG {catalog} TO {q}")
        out += [f"GRANT USE SCHEMA ON SCHEMA {catalog}.{s} TO {q}" for s in schemas]
        out += [f"GRANT SELECT ON TABLE {o} TO {q}" for o in objects]
    return out


def analyst_groups(groups: Mapping[str, str], admin_group: str) -> List[str]:
    return [g for g in groups if g != admin_group]
