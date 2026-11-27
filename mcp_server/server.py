"""MCP server that lets an AI assistant query the crypto market dbt marts.

Tools are read-only and return structured JSON with an `as_of` date and the
source table, so the assistant can cite where every number came from.
"""

from __future__ import annotations

import datetime as dt
from typing import Annotated, Any, Literal

import psycopg2
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

from .catalog import DATE_COLUMNS, MARTS
from .db import ReadOnlyDatabase
from .sql_guard import UnsafeQueryError, check_readonly_query

INSTRUCTIONS = """\
Read-only access to a crypto market data warehouse built from CoinGecko daily data
(15 large-cap coins, 90-day rolling window, plus exchange volume snapshots) and
transformed by dbt into tested mart tables.

Start with list_datasets to see tables, definitions and date coverage. Prefer the
purpose-built tools; use run_readonly_query only when they can't answer the question.
When you report numbers, state the as_of date and source table returned by the tool.
This is market data for analysis, not investment advice.
"""

READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)

mcp = MCPServer(name="crypto-market-intelligence", instructions=INSTRUCTIONS, version="0.1.0")
db = ReadOnlyDatabase()

OptionalDate = Annotated[dt.date | None, Field(description="YYYY-MM-DD. Defaults to the latest date available.")]


def _query(sql: str, params: Any = None, max_rows: int = 500):
    try:
        return db.query(sql, params, max_rows=max_rows)
    except psycopg2.OperationalError as err:
        raise ToolError(
            "Could not reach the warehouse. Is the Docker stack running and has the "
            f"Airflow DAG populated the marts? ({str(err).strip().splitlines()[0]})"
        ) from err
    except psycopg2.errors.UndefinedTable as err:
        raise ToolError("Mart tables not found: run the crypto_market_daily DAG (or `dbt run`) first.") from err
    except psycopg2.errors.QueryCanceled as err:
        raise ToolError("Query took longer than the statement timeout; narrow the date range or filters.") from err


def _resolve_date(table: str, as_of: dt.date | None) -> str:
    """Latest date in `table` on or before as_of (or overall latest)."""
    col = DATE_COLUMNS[table]
    if as_of is None:
        res = _query(f"SELECT max({col}) AS d FROM {table}")
    else:
        res = _query(f"SELECT max({col}) AS d FROM {table} WHERE {col} <= %s", (as_of,))
    value = res.rows[0]["d"] if res.rows else None
    if value is None:
        raise ToolError(f"No data in {table}" + (f" on or before {as_of}." if as_of else "."))
    return value


def _known_coins() -> list[str]:
    return [r["coin_id"] for r in _query("SELECT DISTINCT coin_id FROM mart_daily_summary ORDER BY 1").rows]


def _check_coin(coin_id: str) -> str:
    coin = coin_id.strip().lower()
    coins = _known_coins()
    if coin not in coins:
        raise ToolError(f"Unknown coin_id '{coin_id}'. Available: {', '.join(coins)}.")
    return coin


@mcp.tool(annotations=READ_ONLY)
def list_datasets() -> dict[str, Any]:
    """List the queryable mart tables with their purpose, grain, column definitions,
    row counts and date coverage. Call this first to learn what data exists."""
    datasets = []
    for table, meta in MARTS.items():
        col = DATE_COLUMNS[table]
        stats = _query(f"SELECT count(*) AS n, min({col}) AS first_date, max({col}) AS last_date FROM {table}").rows[0]
        types = _query(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'analytics' AND table_name = %s ORDER BY ordinal_position",
            (table,),
        ).rows
        datasets.append(
            {
                "table": table,
                "description": meta["description"],
                "grain": meta["grain"],
                "row_count": stats["n"],
                "date_range": {"first": stats["first_date"], "last": stats["last_date"]},
                "columns": [
                    {"name": t["column_name"], "type": t["data_type"], "definition": meta["columns"].get(t["column_name"], "")}
                    for t in types
                ],
            }
        )
    return {"datasets": datasets}


@mcp.tool(annotations=READ_ONLY)
def get_market_snapshot(as_of: OptionalDate = None) -> dict[str, Any]:
    """One-day cross-section of every tracked coin: price, daily return, market and
    volatility regime, volume-anomaly flag and drawdown from peak, plus a count of
    coins in each regime. Good for 'how is the market doing today?'."""
    day = _resolve_date("mart_daily_summary", as_of)
    coins = _query(
        """
        SELECT coin_id, price_usd, round((daily_return * 100)::numeric, 2) AS daily_return_pct,
               market_regime, volatility_regime, volume_flag,
               round(volume_z_score::numeric, 2) AS volume_z_score,
               drawdown_pct, drawdown_severity
        FROM mart_daily_summary WHERE price_date = %s
        ORDER BY daily_return DESC NULLS LAST
        """,
        (day,),
    ).rows
    regimes: dict[str, int] = {}
    for c in coins:
        regimes[c["market_regime"]] = regimes.get(c["market_regime"], 0) + 1
    return {"as_of": day, "source": "analytics.mart_daily_summary", "regime_counts": regimes, "coins": coins}


@mcp.tool(annotations=READ_ONLY)
def get_coin_history(
    coin_id: Annotated[str, Field(description="CoinGecko id, e.g. 'bitcoin', 'ethereum', 'solana'")],
    start_date: OptionalDate = None,
    end_date: OptionalDate = None,
) -> dict[str, Any]:
    """Daily time series for one coin (price, return, regimes, volume flag, drawdown)
    over the 90-day window, optionally limited to a date range."""
    coin = _check_coin(coin_id)
    if start_date and end_date and start_date > end_date:
        raise ToolError("start_date must be on or before end_date.")
    res = _query(
        """
        SELECT price_date, price_usd, round((daily_return * 100)::numeric, 2) AS daily_return_pct,
               market_regime, volatility_regime, volume_usd, volume_flag, drawdown_pct
        FROM mart_daily_summary
        WHERE coin_id = %(coin)s
          AND (%(start)s::date IS NULL OR price_date >= %(start)s::date)
          AND (%(end)s::date IS NULL OR price_date <= %(end)s::date)
        ORDER BY price_date
        """,
        {"coin": coin, "start": start_date, "end": end_date},
        max_rows=120,
    )
    return {"coin_id": coin, "source": "analytics.mart_daily_summary", "rows": res.rows, "truncated": res.truncated}


@mcp.tool(annotations=READ_ONLY)
def find_volume_anomalies(
    coin_id: Annotated[str | None, Field(description="Limit to one coin; omit for all coins")] = None,
    direction: Annotated[Literal["high", "low", "any"], Field(description="Volume spikes, droughts, or both")] = "any",
    start_date: OptionalDate = None,
    end_date: OptionalDate = None,
    limit: Annotated[int, Field(ge=1, le=100)] = 25,
) -> dict[str, Any]:
    """Days where a coin's trading volume was more than 2 standard deviations away
    from its 30-day mean, largest deviations first. Useful for spotting market
    events, large-holder activity or exchange outages."""
    coin = _check_coin(coin_id) if coin_id else None
    flags = {"high": ["high_anomaly"], "low": ["low_anomaly"], "any": ["high_anomaly", "low_anomaly"]}[direction]
    res = _query(
        """
        SELECT coin_id, price_date, volume_flag, round(volume_z_score::numeric, 2) AS volume_z_score,
               volume_usd, round((daily_return * 100)::numeric, 2) AS daily_return_pct, market_regime
        FROM mart_daily_summary
        WHERE volume_flag = ANY(%(flags)s)
          AND (%(coin)s::text IS NULL OR coin_id = %(coin)s::text)
          AND (%(start)s::date IS NULL OR price_date >= %(start)s::date)
          AND (%(end)s::date IS NULL OR price_date <= %(end)s::date)
        ORDER BY abs(volume_z_score) DESC, price_date DESC
        LIMIT %(limit)s
        """,
        {"flags": flags, "coin": coin, "start": start_date, "end": end_date, "limit": limit},
    )
    return {"source": "analytics.mart_daily_summary", "rule": "|volume - 30d mean| > 2 * 30d stddev", "anomalies": res.rows}


@mcp.tool(annotations=READ_ONLY)
def get_exchange_market_share(
    snapshot_date: OptionalDate = None,
    top_n: Annotated[int, Field(ge=1, le=50)] = 10,
) -> dict[str, Any]:
    """Exchange share of 24h BTC trading volume on one snapshot date, with market
    concentration: top-3 combined share and the Herfindahl-Hirschman Index
    (sum of squared % shares, 0-10,000; the 2023 US Merger Guidelines treat above 1,800 as highly concentrated)."""
    day = _resolve_date("mart_exchange_market_share", snapshot_date)
    top = _query(
        """
        SELECT volume_rank, exchange_name, market_share_pct, volume_btc_24h, trust_score
        FROM mart_exchange_market_share WHERE snapshot_date = %s
        ORDER BY volume_rank LIMIT %s
        """,
        (day, top_n),
    ).rows
    conc = _query(
        """
        SELECT count(*) AS exchanges,
               round(sum(market_share_pct * market_share_pct)::numeric, 0) AS hhi,
               round(sum(market_share_pct) FILTER (WHERE volume_rank <= 3)::numeric, 2) AS top3_share_pct
        FROM mart_exchange_market_share WHERE snapshot_date = %s
        """,
        (day,),
    ).rows[0]
    return {"as_of": day, "source": "analytics.mart_exchange_market_share", "concentration": conc, "exchanges": top}


@mcp.tool(annotations=READ_ONLY)
def get_btc_correlation(as_of: OptionalDate = None) -> dict[str, Any]:
    """30-day rolling correlation of each altcoin's daily returns with Bitcoin on one
    date, highest first. High values mean holding altcoins adds little diversification."""
    day = _resolve_date("mart_cross_asset_correlation", as_of)
    rows = _query(
        """
        SELECT coin_id, round(correlation_30d::numeric, 3) AS correlation_30d, correlation_regime
        FROM mart_cross_asset_correlation WHERE price_date = %s
        ORDER BY correlation_30d DESC NULLS LAST
        """,
        (day,),
    ).rows
    return {"as_of": day, "source": "analytics.mart_cross_asset_correlation", "coins": rows}


@mcp.tool(annotations=READ_ONLY)
def run_readonly_query(
    sql: Annotated[str, Field(description="One PostgreSQL SELECT over the analytics mart tables")],
    max_rows: Annotated[int, Field(ge=1, le=1000)] = 200,
) -> dict[str, Any]:
    """Run a custom read-only SELECT against the mart tables when no other tool fits.
    Only mart_daily_summary, mart_market_regime, mart_exchange_market_share and
    mart_cross_asset_correlation are allowed; writes, DDL and system functions are rejected."""
    try:
        checked = check_readonly_query(sql)
    except UnsafeQueryError as err:
        raise ToolError(str(err)) from err
    try:
        res = _query(checked.sql, max_rows=max_rows)
    except psycopg2.Error as err:
        raise ToolError(f"Postgres rejected the query: {str(err).strip().splitlines()[0]}") from err
    return {
        "tables": sorted(checked.tables),
        "columns": res.columns,
        "rows": res.rows,
        "row_count": len(res.rows),
        "truncated": res.truncated,
    }


@mcp.prompt()
def market_briefing(as_of: str = "") -> str:
    """Draft a short market briefing grounded in the warehouse."""
    when = f"as of {as_of}" if as_of else "for the latest date available"
    return (
        f"Write a 150-word crypto market briefing {when} for a sales team preparing client calls. "
        "Use get_market_snapshot, find_volume_anomalies (direction 'high', last 7 days), "
        "get_exchange_market_share and get_btc_correlation. Lead with the regime mix, call out "
        "unusual volume and venue concentration, cite the as_of date and source table for each "
        "number, and do not give investment advice."
    )
