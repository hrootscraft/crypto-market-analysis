"""Postgres loader for crypto market data pipeline.

Handles upserting (insert and update) extracted data into the warehouse.
Uses INSERT ... ON CONFLICT for idempotent loads -> safe to re-run
pipeline multiple times without creating duplicates in the warehouse.
"""

import logging
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def get_connection(host: str, dbname: str, user: str, password: str, port: int):
    """Create a Postgres connection using provided credentials.

    Args:
        host: Database hostname or IP address
        dbname: Database name
        user: Database user
        password: Database password
        port: Database port

    Returns:
        psycopg2 connection object to the Postgres database
    """
    return psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
    )


def load_daily_prices(conn, rows: list[dict[str, Any]]) -> int:
    """Upsert daily price data into raw.daily_prices.

    Uses INSERT ... ON CONFLICT to handle re-runs idempotently.
    On conflict, updates price, market_cap, and volume to latest values.

    Args:
        conn: Active psycopg2 connection.
        rows: List of dicts from extract_coin_history().

    Returns:
        Number of rows upserted.
    """
    if not rows:
        logger.warning("No price rows to load")
        return 0

    sql = """
        INSERT INTO raw.daily_prices 
            (coin_id, price_date, price_usd, market_cap_usd, volume_usd)
        VALUES %s
        ON CONFLICT (coin_id, price_date) DO UPDATE SET
            price_usd      = EXCLUDED.price_usd,
            market_cap_usd = EXCLUDED.market_cap_usd,
            volume_usd     = EXCLUDED.volume_usd,
            ingested_at    = NOW()
    """
    # Deduplicate: CoinGecko may return duplicate dates in a single response.
    # Keep the last occurrence (most recent data point for that date).
    seen = {}
    for r in rows:
        key = (r["coin_id"], r["price_date"])  # conflict key
        seen[key] = (
            r["coin_id"],
            r["price_date"],
            r["price_usd"],
            r["market_cap_usd"],
            r["volume_usd"],
        )
    values = list(seen.values())

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()

    logger.info("Upserted %d price rows", len(values))
    return len(values)


def load_exchange_volumes(conn, rows: list[dict[str, Any]]) -> int:
    """Upsert exchange volume data into raw.exchange_volumes.

    Uses INSERT ... ON CONFLICT to handle re-runs idempotently.
    On conflict, updates exchange_name, volume, and trust_score to latest values.

    Args:
        conn: Active psycopg2 connection.
        rows: List of dicts from extract_exchange_volumes().

    Returns:
        Number of rows upserted.
    """
    if not rows:
        logger.warning("No exchange rows to load")
        return 0

    sql = """
        INSERT INTO raw.exchange_volumes 
            (exchange_id, exchange_name, volume_btc_24h, trust_score, snapshot_date)
        VALUES %s
        ON CONFLICT (exchange_id, snapshot_date) DO UPDATE SET
            exchange_name  = EXCLUDED.exchange_name,
            volume_btc_24h = EXCLUDED.volume_btc_24h,
            trust_score    = EXCLUDED.trust_score,
            ingested_at    = NOW()
    """
    values = [
        (
            r["exchange_id"],
            r["exchange_name"],
            r["volume_btc_24h"],
            r["trust_score"],
            r["snapshot_date"],
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()

    logger.info("Upserted %d exchange rows", len(values))
    return len(values)
