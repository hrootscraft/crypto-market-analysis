"""Validation for free-form SQL sent to the MCP server by an AI client.

This is the first of three layers that keep model-written SQL read-only:

1. This module: parse the SQL and accept only a single SELECT over the
   curated dbt mart tables (no writes, no locks, no system functions).
2. db.py: run it inside a READ ONLY transaction with a statement timeout.
3. Postgres: connect as `mcp_reader`, a role that only has SELECT on the
   `analytics` schema (see sql/create_mcp_reader_role.sql).

Any one layer could be bypassed by a bug; the point is that all three would
have to fail at once for a model to change data.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

# Only the tested, documented mart tables are queryable. Staging/intermediate
# models and the raw schema are deliberately out of scope so that answers are
# grounded in the same tables the Superset dashboards use.
ALLOWED_SCHEMA = "analytics"
ALLOWED_TABLES = frozenset(
    {
        "mart_daily_summary",
        "mart_market_regime",
        "mart_exchange_market_share",
        "mart_cross_asset_correlation",
    }
)

# Statement types that change data, schema, permissions or session state.
_FORBIDDEN_NODES: tuple[type[exp.Expression], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Command,  # anything sqlglot can't model: COPY, GRANT, VACUUM, DO, ...
    exp.Set,
    exp.Transaction,
    exp.Commit,
    exp.Rollback,
    exp.Lock,  # SELECT ... FOR UPDATE / FOR SHARE
)

# Functions that read server files, sleep, touch other sessions or change
# settings. The read-only role cannot use most of them anyway.
_FORBIDDEN_FUNCTIONS = frozenset(
    {
        "pg_sleep",
        "pg_sleep_for",
        "pg_sleep_until",
        "pg_read_file",
        "pg_read_binary_file",
        "pg_ls_dir",
        "pg_stat_file",
        "lo_import",
        "lo_export",
        "dblink",
        "dblink_exec",
        "set_config",
        "pg_terminate_backend",
        "pg_cancel_backend",
        "pg_reload_conf",
        "pg_advisory_lock",
        "pg_advisory_xact_lock",
        "query_to_xml",
    }
)

_ALLOWED_ROOTS: tuple[type[exp.Expression], ...] = (exp.Select, exp.SetOperation)


class UnsafeQueryError(ValueError):
    """The SQL is not a single read-only query over the allowed tables."""


@dataclass(frozen=True)
class CheckedQuery:
    sql: str
    tables: frozenset[str]


def _function_name(node: exp.Func) -> str:
    if isinstance(node, exp.Anonymous):
        return str(node.this).lower()
    return node.sql_name().lower()


def check_readonly_query(sql: str) -> CheckedQuery:
    """Return the cleaned query if it is safe to run, else raise UnsafeQueryError."""
    cleaned = (sql or "").strip().rstrip(";").strip()
    if not cleaned:
        raise UnsafeQueryError("Query is empty.")

    try:
        statements = [s for s in sqlglot.parse(cleaned, read="postgres") if s is not None]
    except ParseError as err:
        raise UnsafeQueryError(f"Could not parse SQL: {err.errors[0]['description'] if err.errors else err}") from err

    if len(statements) != 1:
        raise UnsafeQueryError("Send exactly one SQL statement.")
    tree = statements[0]

    if not isinstance(tree, _ALLOWED_ROOTS):
        raise UnsafeQueryError(
            f"{tree.key.upper()} statements are not allowed; send a single SELECT (WITH / UNION are fine)."
        )

    for node in tree.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            raise UnsafeQueryError(f"{node.key.upper()} is not allowed; this server is read-only.")
        if isinstance(node, exp.Select) and node.args.get("into") is not None:
            raise UnsafeQueryError("SELECT ... INTO creates a table and is not allowed.")
        if isinstance(node, exp.Func) and _function_name(node) in _FORBIDDEN_FUNCTIONS:
            raise UnsafeQueryError(f"Function {_function_name(node)}() is not allowed.")

    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    tables: set[str] = set()
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        schema = (table.db or "").lower()
        if not schema and name in cte_names:
            continue
        if table.catalog:
            raise UnsafeQueryError("Cross-database references are not allowed.")
        if schema and schema != ALLOWED_SCHEMA:
            raise UnsafeQueryError(f"Schema '{schema}' is not allowed; query the '{ALLOWED_SCHEMA}' marts only.")
        if name not in ALLOWED_TABLES:
            raise UnsafeQueryError(
                f"Table '{name}' is not queryable. Allowed tables: {', '.join(sorted(ALLOWED_TABLES))}."
            )
        tables.add(name)

    if not tables:
        raise UnsafeQueryError("Query must read from at least one mart table.")

    return CheckedQuery(sql=cleaned, tables=frozenset(tables))
