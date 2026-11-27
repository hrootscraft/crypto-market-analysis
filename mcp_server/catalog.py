"""Business descriptions of the dbt mart tables exposed over MCP.

Kept in step with dbt_project/models/marts/_marts.yml (a test checks that the
table list matches). The descriptions are what an AI client reads to decide
which table answers a question, so they say what each table is *for*, its
grain, and the business rules behind each derived column.
"""

from __future__ import annotations

MARTS: dict[str, dict] = {
    "mart_daily_summary": {
        "description": (
            "Dashboard-ready daily view per coin: price, return, market and volatility "
            "regime, volume-anomaly flag and drawdown from peak. Primary table for "
            "Superset dashboards and the best starting point for most questions."
        ),
        "grain": "one row per coin per day (rolling 90-day window)",
        "columns": {
            "coin_id": "CoinGecko coin id, e.g. bitcoin, ethereum, solana",
            "price_date": "UTC calendar date",
            "price_usd": "closing price in USD",
            "volume_usd": "24h trading volume in USD",
            "daily_return": "day-over-day return as a fraction (0.05 = +5%)",
            "market_regime": "bull if 7d and 30d average returns are both > 0, bear if both < 0, else sideways",
            "volatility_regime": "high_vol if 30d return stddev > 5%, medium_vol if > 2%, else low_vol",
            "volatility_30d": "30-day rolling standard deviation of daily returns",
            "volume_flag": "high_anomaly / low_anomaly when volume is more than 2 standard deviations from its 30-day mean, else normal",
            "volume_z_score": "standard deviations between today's volume and the 30-day mean",
            "running_max_price": "highest price so far in the window",
            "drawdown_pct": "percent below running peak (0 = at peak, -20 = 20% below)",
            "drawdown_severity": "critical (< -30%), moderate (-30% to -15%), mild (> -15%)",
        },
    },
    "mart_market_regime": {
        "description": (
            "Market regime classification per coin and day from 7d/30d average returns "
            "and 30d volatility. Use to control for market conditions."
        ),
        "grain": "one row per coin per day",
        "columns": {
            "coin_id": "CoinGecko coin id",
            "price_date": "UTC calendar date",
            "price_usd": "closing price in USD",
            "volume_usd": "24h trading volume in USD",
            "daily_return": "day-over-day return as a fraction",
            "avg_return_7d": "7-day rolling mean of daily returns",
            "avg_return_30d": "30-day rolling mean of daily returns",
            "volatility_7d": "7-day rolling standard deviation of daily returns",
            "volatility_30d": "30-day rolling standard deviation of daily returns",
            "market_regime": "bull / bear / sideways",
            "volatility_regime": "high_vol / medium_vol / low_vol",
        },
    },
    "mart_exchange_market_share": {
        "description": (
            "Each exchange's share of total 24h BTC trading volume across tracked "
            "exchanges, with daily volume rank. Use for competitive dynamics between venues."
        ),
        "grain": "one row per exchange per snapshot date",
        "columns": {
            "exchange_id": "CoinGecko exchange id",
            "exchange_name": "display name",
            "snapshot_date": "date the volume snapshot was taken",
            "volume_btc_24h": "24h trading volume in BTC",
            "trust_score": "CoinGecko trust score (1-10)",
            "total_volume_btc": "sum of volume across tracked exchanges that day",
            "market_share_pct": "exchange volume / total volume * 100",
            "volume_rank": "1 = largest exchange by volume that day",
        },
    },
    "mart_cross_asset_correlation": {
        "description": (
            "Rolling 30-day Pearson correlation of each altcoin's daily returns with "
            "Bitcoin's. High correlation means little diversification benefit."
        ),
        "grain": "one row per non-BTC coin per day",
        "columns": {
            "coin_id": "CoinGecko coin id (never bitcoin)",
            "price_date": "UTC calendar date",
            "alt_return": "the coin's daily return",
            "btc_return": "Bitcoin's daily return on the same day",
            "correlation_30d": "30-day rolling correlation with BTC, -1 to 1",
            "correlation_regime": "high (>= 0.8), moderate (>= 0.4), low (>= 0), negative",
        },
    },
}

# Date column used to report data freshness for each mart.
DATE_COLUMNS = {
    "mart_daily_summary": "price_date",
    "mart_market_regime": "price_date",
    "mart_exchange_market_share": "snapshot_date",
    "mart_cross_asset_correlation": "price_date",
}
