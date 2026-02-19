"""
Fetch daily price/volume data and exchange volume data with retry logic and rate-limit awareness
CoinGecko free tier: ~10-30 calls/minute. I add polite delays and exponential backoff to stay within limits
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout

logger = logging.getLogger(__name__)


def _request_with_retry(
    url: str,
    params: dict,
    max_retries: int = 3,
    backoff_seconds: float = 2.0,
) -> Any:
    for attempt in range(1, max_retries + 1):
        try:
            logger.debug("GET %s (attempt %d/%d)", url, attempt, max_retries)
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        except (HTTPError, ConnectionError, Timeout) as e:
            is_rate_limit = (
                hasattr(e, "response")
                and e.response is not None
                and e.response.status_code == 429
            )
            # 60s wait for rate limits, exponential backoff (wait 2s, then 4s, then 8s) for other errors
            wait = 60 if is_rate_limit else backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Attempt %d/%d failed for %s: %s. Retrying in %.1fs",
                attempt,
                max_retries,
                url,
                e,
                wait,
            )
            if attempt == max_retries:
                logger.error("All %d retries exhausted for %s", max_retries, url)
                raise
            time.sleep(wait)


def extract_coin_history(
    base_url: str,
    coin_id: str,
    vs_currency: str = "usd",
    days: int = 90,
    max_retries: int = 3,
    backoff_seconds: float = 2.0,
) -> list[dict[str, Any]]:
    """Extract daily price and volume history for a single coin.

    Args:
        base_url: CoinGecko API base URL e.g., "https://api.coingecko.com/api/v3"
        coin_id: CoinGecko coin identifier e.g., "bitcoin", "ethereum"
        vs_currency: Quote currency for prices
        days: Number of days of history to fetch
        max_retries: Maximum retry attempts on transient failures
        backoff_seconds: Base delay for exponential backoff on failures

    Returns:
        List of dicts with keys:
        coin_id,
        price_date,
        price_usd,
        market_cap_usd (total value of all coins in existence multiplied by price on that day, can be None if data is missing for that day),
        volume_usd (total USD traded on that day across all exchanges, can be None if data is missing for that day).

    Raises:
        requests.HTTPError: If all retries exhausted or non-retryable error occurs

    Example:
        >>> rows = extract_coin_history(
        ...     "https://api.coingecko.com/api/v3", "bitcoin", days=7
        ... )
        >>> rows[0].keys()
        dict_keys(['coin_id', 'price_date', 'price_usd', 'market_cap_usd', 'volume_usd'])
    """
    url = f"{base_url}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days, "interval": "daily"}

    data = _request_with_retry(url, params, max_retries, backoff_seconds)

    prices = data.get("prices", [])
    market_caps = data.get("market_caps", [])
    volumes = data.get("total_volumes", [])

    rows = []
    for i, (ts, price) in enumerate(prices):
        rows.append(
            {
                "coin_id": coin_id,
                "price_date": datetime.fromtimestamp(
                    ts / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d"),
                "price_usd": price,
                "market_cap_usd": market_caps[i][1] if i < len(market_caps) else None,
                "volume_usd": volumes[i][1] if i < len(volumes) else None,
            }
        )

    logger.info(
        "Extracted %d daily records for %s (%d days)",
        len(rows),
        coin_id,
        days,
    )
    return rows


def extract_exchange_volumes(
    base_url: str,
    max_retries: int = 3,
    backoff_seconds: float = 2.0,
) -> list[dict[str, Any]]:
    """Extract 24h volume data for top 10 exchanges.

    Returns:
        List of dicts with keys:
        exchange_id,
        exchange_name (e.g. Binance, Kraken),
        volume_btc_24h (how much was traded on that exchange in the last 24h, denominated in BTC),
        trust_score (a score from CoinGecko indicating the reliability of the exchange based on factors like liquidity, API uptime, regulatory compliance, etc.),
        snapshot_date (date when the data was extracted, in YYYY-MM-DD format).
    """
    url = f"{base_url}/exchanges"
    params = {"per_page": 10, "page": 1}

    data = _request_with_retry(url, params, max_retries, backoff_seconds)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = []
    for ex in data:
        rows.append(
            {
                "exchange_id": ex.get("id"),
                "exchange_name": ex.get("name"),
                "volume_btc_24h": ex.get("trade_volume_24h_btc"),
                "trust_score": ex.get("trust_score"),
                "snapshot_date": today,
            }
        )

    logger.info("Extracted volume data for %d exchanges", len(rows))
    return rows
