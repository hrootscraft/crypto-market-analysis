"""
Daily ELT pipeline: CoinGecko API → Postgres → dbt → tests.

Schedule: 06:00 UTC
Flow: extract 90 days of prices + exchange volumes (parallel)
      → dbt deps → source freshness → dbt build --fail-fast
"""

import logging
import sys
import time
from datetime import datetime, timedelta
from os import environ

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Make modules importable inside the Airflow container
sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

# Configuration from environment variables (defined in docker-compose.yml)
PG_HOST = environ.get("POSTGRES_HOST", "warehouse")
PG_DB = environ.get("POSTGRES_DB", "crypto")
PG_USER = environ.get("POSTGRES_USER", "crypto_user")
PG_PASS = environ.get("POSTGRES_PASSWORD", "crypto_pass")
PG_PORT = int(environ.get("POSTGRES_PORT", "5432"))

COINGECKO_URL = environ.get("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")

COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
    "avalanche-2",
    "polkadot",
    "chainlink",
    "dogecoin",
    "ripple",
    "tron",
    "litecoin",
    "polygon-ecosystem-token",
    "uniswap",
    "stellar",
    "cosmos",
]


# Task callables
def extract_and_load_prices(**context):
    from extract.coingecko import extract_coin_history
    from extract.loader import get_connection, load_daily_prices

    conn = get_connection(PG_HOST, PG_DB, PG_USER, PG_PASS, PG_PORT)
    total_rows = 0

    try:
        failed_coins = []
        for i, coin in enumerate(COINS):
            logger.info("Processing coin %d/%d: %s", i + 1, len(COINS), coin)
            try:
                rows = extract_coin_history(
                    base_url=COINGECKO_URL,
                    coin_id=coin,
                    days=90,
                )
                loaded = load_daily_prices(conn, rows)
                total_rows += loaded
            except Exception as e:
                logger.warning("Skipping %s due to error: %s", coin, e)
                failed_coins.append(coin)

            # CoinGecko free tier: ~10 calls/min, 12s pause keeps us safe
            if i < len(COINS) - 1:
                time.sleep(12)

        if failed_coins:
            logger.warning(
                "Failed coins (%d/%d): %s", len(failed_coins), len(COINS), failed_coins
            )
            if len(failed_coins) > len(COINS) // 2:
                raise RuntimeError(f"Too many failures: {failed_coins}")

        logger.info("Total price rows loaded: %d", total_rows)
    finally:
        conn.close()

    return total_rows


def extract_and_load_exchanges(**context):
    from extract.coingecko import extract_exchange_volumes
    from extract.loader import get_connection, load_exchange_volumes

    conn = get_connection(PG_HOST, PG_DB, PG_USER, PG_PASS, PG_PORT)
    try:
        rows = extract_exchange_volumes(base_url=COINGECKO_URL)
        loaded = load_exchange_volumes(conn, rows)
        logger.info("Exchange rows loaded: %d", loaded)
    finally:
        conn.close()

    return loaded


# DAG definition
default_args = {
    "owner": "rutuja",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="crypto_market_daily",
    description="Daily ingestion of crypto prices and exchange volumes, followed by dbt transformation and testing.",
    start_date=datetime(2026, 2, 17),
    schedule="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=[
        "crypto",
        "coingecko",
        "dbt",
        "postgres",
        "data-warehouse",
        "elt",
        "orchestration",
        "daily",
    ],
) as dag:

    # Extract & Load
    load_prices = PythonOperator(
        task_id="extract_load_prices",
        python_callable=extract_and_load_prices,
    )

    load_exchanges = PythonOperator(
        task_id="extract_load_exchanges",
        python_callable=extract_and_load_exchanges,
    )

    # Install dbt packages (required before any dbt command can parse the project)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt_project && dbt deps --profiles-dir . --log-path /tmp/dbt_logs",
    )

    # Source freshness check (are raw tables up to date?)
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command="cd /opt/airflow/dbt_project && dbt source freshness --profiles-dir . --target prod --log-path /tmp/dbt_logs",
    )

    # Build models and run tests in DAG order (test each model right after it's built)
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/dbt_project && dbt build --fail-fast --profiles-dir . --target prod --log-path /tmp/dbt_logs",
    )

    # Dependencies: extract in parallel → install deps → check freshness → build (run + test)
    ([load_prices, load_exchanges] >> dbt_deps >> dbt_source_freshness >> dbt_build)
