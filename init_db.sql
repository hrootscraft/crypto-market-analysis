-- Initialize the crypto data warehouse
-- This runs automatically when the Postgres container starts for the first time

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw daily price/volume data from CoinGecko; one row per coin per day, with USD price, market cap, and volume
CREATE TABLE IF NOT EXISTS raw.daily_prices (
    coin_id         VARCHAR(50)     NOT NULL,
    price_date      DATE            NOT NULL,
    price_usd       NUMERIC(20, 8) NOT NULL,
    market_cap_usd  NUMERIC(24, 2),
    volume_usd      NUMERIC(24, 2) NOT NULL,
    ingested_at     TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (coin_id, price_date)
);

-- Raw exchange volume data from CoinGecko; one row per exchange per day, with 24h BTC volume and trust score
CREATE TABLE IF NOT EXISTS raw.exchange_volumes (
    exchange_id         VARCHAR(100)    NOT NULL,
    exchange_name       VARCHAR(200)    NOT NULL,
    volume_btc_24h      NUMERIC(24, 8),
    trust_score         INTEGER,
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    snapshot_date       DATE            NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (exchange_id, snapshot_date)
);

-- Single-column indexes for queries that filter on just date or just coin
-- (the PK already covers the composite case)
CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON raw.daily_prices (price_date);
CREATE INDEX IF NOT EXISTS idx_daily_prices_coin ON raw.daily_prices (coin_id);
