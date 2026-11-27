"""Unit tests for the MCP server's SQL guard. No database needed.

Run with: pytest tests/test_mcp_sql_guard.py -v
"""
from pathlib import Path

import pytest

from mcp_server.catalog import DATE_COLUMNS, MARTS
from mcp_server.sql_guard import ALLOWED_TABLES, UnsafeQueryError, check_readonly_query

ALLOWED = [
    "SELECT * FROM mart_daily_summary",
    "select coin_id, avg(daily_return) from analytics.mart_market_regime group by 1;",
    "WITH latest AS (SELECT max(price_date) d FROM mart_daily_summary) "
    "SELECT s.* FROM mart_daily_summary s JOIN latest l ON s.price_date = l.d",
    "SELECT coin_id FROM mart_daily_summary UNION SELECT coin_id FROM mart_cross_asset_correlation",
    "SELECT exchange_name, market_share_pct FROM mart_exchange_market_share WHERE volume_rank <= 3",
    "SELECT coin_id, rank() OVER (ORDER BY drawdown_pct) FROM mart_daily_summary",
]


@pytest.mark.parametrize("sql", ALLOWED)
def test_allows_read_only_mart_queries(sql):
    assert check_readonly_query(sql).tables <= ALLOWED_TABLES


def test_reports_tables_and_ignores_cte_names():
    checked = check_readonly_query(
        "WITH x AS (SELECT * FROM mart_daily_summary) SELECT * FROM x JOIN mart_market_regime USING (coin_id)"
    )
    assert checked.tables == {"mart_daily_summary", "mart_market_regime"}


BLOCKED = {
    "delete": "DELETE FROM mart_daily_summary",
    "update": "UPDATE mart_daily_summary SET price_usd = 0",
    "insert": "INSERT INTO mart_daily_summary (coin_id) VALUES ('x')",
    "drop": "DROP TABLE mart_daily_summary",
    "truncate": "TRUNCATE mart_daily_summary",
    "create": "CREATE TABLE analytics.notes (a int)",
    "grant": "GRANT ALL ON mart_daily_summary TO public",
    "copy": "COPY mart_daily_summary TO '/tmp/out.csv'",
    "two statements": "SELECT 1 FROM mart_daily_summary; DROP TABLE mart_daily_summary",
    "writable cte": "WITH d AS (DELETE FROM mart_daily_summary RETURNING *) SELECT * FROM d",
    "select into": "SELECT * INTO analytics.copy FROM mart_daily_summary",
    "row lock": "SELECT * FROM mart_daily_summary FOR UPDATE",
    "set": "SET search_path = raw",
    "raw schema": "SELECT * FROM raw.daily_prices",
    "staging view": "SELECT * FROM stg_daily_prices",
    "system catalog": "SELECT * FROM pg_catalog.pg_roles",
    "information schema": "SELECT * FROM information_schema.tables",
    "other database": "SELECT * FROM otherdb.analytics.mart_daily_summary",
    "sleep": "SELECT pg_sleep(60) FROM mart_daily_summary",
    "read file": "SELECT pg_read_file('/etc/passwd') FROM mart_daily_summary",
    "set_config": "SELECT set_config('search_path', 'raw', false) FROM mart_daily_summary",
    "no table": "SELECT 1",
    "do block": "DO $$ BEGIN PERFORM 1; END $$",
    "empty": "   ;  ",
    "garbage": "SELEKT * FRUM mart_daily_summary",
}


@pytest.mark.parametrize("sql", BLOCKED.values(), ids=BLOCKED.keys())
def test_blocks_unsafe_queries(sql):
    with pytest.raises(UnsafeQueryError):
        check_readonly_query(sql)


def test_error_message_lists_allowed_tables():
    with pytest.raises(UnsafeQueryError, match="mart_daily_summary"):
        check_readonly_query("SELECT * FROM stg_daily_prices")


def test_catalog_matches_dbt_marts():
    """Every documented mart is exposed, and nothing else is."""
    yaml = pytest.importorskip("yaml")
    marts_yml = Path(__file__).resolve().parents[1] / "dbt_project" / "models" / "marts" / "_marts.yml"
    dbt_marts = {m["name"] for m in yaml.safe_load(marts_yml.read_text())["models"]}
    assert set(MARTS) == dbt_marts == set(ALLOWED_TABLES) == set(DATE_COLUMNS)
