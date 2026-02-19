-- Staging layer: get the latest daily price/volume data for each coin, filtering out any rows with invalid prices or volumes
-- Source: raw.daily_prices (CoinGecko API)
-- Grain: one row per coin per day with price > 0 and volume >= 0
with source as (
    select coin_id,
        price_date,
        price_usd,
        market_cap_usd,
        volume_usd,
        ingested_at,
        row_number() over (
            partition by coin_id,
            price_date
            order by ingested_at desc
        ) as row_num
    from {{source('raw', 'daily_prices')}}
    where price_usd > 0
        and volume_usd >= 0
)
select coin_id,
    price_date,
    price_usd,
    market_cap_usd,
    volume_usd
from source
where row_num = 1
    -- Trim to exactly 90 calendar days from the most recent date.
    -- CoinGecko's days=90 param often returns a few extra days;
    -- without this guard every rolling metric, regime count, and
    -- anomaly tally is based on an unpredictable window width.
    and price_date > (
        select max(price_date) - interval '90 days'
        from {{source('raw', 'daily_prices')}}
    )
    
-- row_number() dedup is defensive: raw PK prevents duplicates today,
-- but this protects downstream models if the ingestion pattern ever changes.