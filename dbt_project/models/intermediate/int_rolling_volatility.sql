-- Intermediate: daily returns and rolling volatility metrics
-- Used by mart models for market regime classification
-- Grain: one row per coin per day
with daily as (
    select coin_id,
        price_date,
        price_usd,
        volume_usd,
        lag(price_usd) over (
            partition by coin_id
            order by price_date
        ) as prev_price
    from {{ref('stg_daily_prices')}}
),
with_returns as (
    select *,
        case
            when prev_price > 0 then (price_usd - prev_price) / prev_price
            else null
        end as daily_return
    from daily
    where prev_price is not null
)
select coin_id,
    price_date,
    price_usd,
    volume_usd,
    daily_return,
    -- 7-day rolling metrics
    avg(daily_return) over (
        partition by coin_id
        order by price_date rows between 6 preceding and current row
    ) as avg_return_7d,
    -- 7-day rolling volatility (stddev of returns)
    stddev(daily_return) over (
        partition by coin_id
        order by price_date rows between 6 preceding and current row
    ) as volatility_7d,
    -- 30-day rolling metrics
    avg(daily_return) over (
        partition by coin_id
        order by price_date rows between 29 preceding and current row
    ) as avg_return_30d,
    -- 30-day rolling volatility (stddev of returns)
    stddev(daily_return) over (
        partition by coin_id
        order by price_date rows between 29 preceding and current row
    ) as volatility_30d,
    -- 30-day rolling average volume (for anomaly detection)
    avg(volume_usd) over (
        partition by coin_id
        order by price_date rows between 29 preceding and current row
    ) as avg_volume_30d,
    -- 30-day rolling stddev of volume (for anomaly detection)
    stddev(volume_usd) over (
        partition by coin_id
        order by price_date rows between 29 preceding and current row
    ) as stddev_volume_30d
from with_returns