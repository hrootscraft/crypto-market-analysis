"""Integration tests: an in-process MCP client calling the server against a real warehouse.

Needs a running warehouse with the marts built and the mcp_reader role created
(see README, "Ask Claude about the market (MCP server)"). Skipped otherwise:

    MCP_INTEGRATION=1 pytest tests/test_mcp_server.py -v
"""
import asyncio
import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("MCP_INTEGRATION") != "1", reason="set MCP_INTEGRATION=1 with a running warehouse"
)

from mcp import Client  # noqa: E402

from mcp_server.server import mcp  # noqa: E402


def call(name, args=None):
    async def _run():
        async with Client(mcp) as client:
            return await client.call_tool(name, args or {})

    return asyncio.run(_run())


def ok(name, args=None):
    result = call(name, args)
    assert not result.is_error, result.content
    return result.structured_content


def error_text(name, args=None):
    result = call(name, args)
    assert result.is_error
    return " ".join(getattr(c, "text", "") for c in result.content)


def test_every_tool_is_declared_read_only():
    async def _run():
        async with Client(mcp) as client:
            return (await client.list_tools()).tools

    tools = asyncio.run(_run())
    assert {t.name for t in tools} >= {"list_datasets", "get_market_snapshot", "run_readonly_query"}
    for tool in tools:
        assert tool.annotations.read_only_hint is True
        assert tool.annotations.destructive_hint is False


def test_list_datasets_describes_all_marts():
    data = ok("list_datasets")
    tables = {d["table"]: d for d in data["datasets"]}
    assert set(tables) == {
        "mart_daily_summary",
        "mart_market_regime",
        "mart_exchange_market_share",
        "mart_cross_asset_correlation",
    }
    summary = tables["mart_daily_summary"]
    assert summary["row_count"] > 0
    assert summary["date_range"]["last"] >= summary["date_range"]["first"]
    assert any(c["name"] == "volume_flag" and c["definition"] for c in summary["columns"])


def test_market_snapshot_is_one_day_with_provenance():
    data = ok("get_market_snapshot")
    assert data["source"] == "analytics.mart_daily_summary"
    assert data["coins"]
    assert sum(data["regime_counts"].values()) == len(data["coins"])
    assert {c["market_regime"] for c in data["coins"]} <= {"bull", "bear", "sideways"}


def test_snapshot_as_of_resolves_to_latest_available_day():
    latest = ok("get_market_snapshot")["as_of"]
    assert ok("get_market_snapshot", {"as_of": "2100-01-01"})["as_of"] == latest
    assert "No data" in error_text("get_market_snapshot", {"as_of": "1990-01-01"})


def test_coin_history_filters_and_validates():
    rows = ok("get_coin_history", {"coin_id": "bitcoin"})["rows"]
    assert len(rows) > 30
    dates = [r["price_date"] for r in rows]
    assert dates == sorted(dates)
    window = ok("get_coin_history", {"coin_id": "Bitcoin", "start_date": dates[-7], "end_date": dates[-1]})
    assert len(window["rows"]) == 7
    assert "Unknown coin_id" in error_text("get_coin_history", {"coin_id": "notacoin"})
    assert "start_date" in error_text("get_coin_history", {"coin_id": "bitcoin", "start_date": dates[-1], "end_date": dates[0]})


def test_volume_anomalies_follow_two_sigma_rule():
    data = ok("find_volume_anomalies", {"direction": "high", "limit": 10})
    for a in data["anomalies"]:
        assert a["volume_flag"] == "high_anomaly"
        assert a["volume_z_score"] > 2
    z = [abs(a["volume_z_score"]) for a in data["anomalies"]]
    assert z == sorted(z, reverse=True)


def test_exchange_market_share_and_concentration():
    data = ok("get_exchange_market_share", {"top_n": 3})
    assert [e["volume_rank"] for e in data["exchanges"]] == [1, 2, 3]
    conc = data["concentration"]
    top3 = sum(e["market_share_pct"] for e in data["exchanges"])
    assert conc["top3_share_pct"] == pytest.approx(top3, abs=0.05)
    assert 0 < conc["hhi"] <= 10000


def test_btc_correlation_excludes_bitcoin():
    data = ok("get_btc_correlation")
    assert data["coins"]
    assert all(c["coin_id"] != "bitcoin" for c in data["coins"])
    assert all(-1 <= c["correlation_30d"] <= 1 for c in data["coins"] if c["correlation_30d"] is not None)


def test_readonly_query_runs_and_truncates():
    data = ok("run_readonly_query", {"sql": "SELECT coin_id, price_date FROM mart_daily_summary", "max_rows": 5})
    assert data["row_count"] == 5 and data["truncated"] is True
    assert data["tables"] == ["mart_daily_summary"]


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM mart_daily_summary",
        "SELECT * FROM raw.daily_prices",
        "WITH d AS (DELETE FROM mart_daily_summary RETURNING *) SELECT * FROM d",
    ],
)
def test_readonly_query_rejects_writes_and_raw(sql):
    assert "not allowed" in error_text("run_readonly_query", {"sql": sql})
    # and the data is untouched
    assert ok("run_readonly_query", {"sql": "SELECT count(*) AS n FROM mart_daily_summary"})["rows"][0]["n"] > 0


def test_database_role_blocks_writes_even_if_guard_is_bypassed():
    """Layer 2 and 3: the connection itself cannot write, independent of the SQL guard."""
    import psycopg2

    from mcp_server.server import db

    with pytest.raises(psycopg2.Error):
        db.query("SELECT * FROM raw.daily_prices LIMIT 1")
    with pytest.raises(psycopg2.Error):
        db.query("DELETE FROM mart_daily_summary RETURNING coin_id")
