-- Singular test: mart_daily_summary must not have more rows than raw.daily_prices for any coin.
--
-- raw.daily_prices has a PRIMARY KEY on (coin_id, price_date), so it contains
-- at most one row per coin per day. mart_daily_summary is expected to have
-- FEWER rows than raw because the 30-day rolling-window startup period is
-- filtered out in mart_market_regime (WHERE avg_return_30d IS NOT NULL).
--
-- If mart_rows > raw_rows for any coin, a join fanout or duplication bug
-- has introduced fabricated rows — every metric for that coin is wrong.
--
-- Returns rows = FAIL. Returns nothing = PASS.
with raw_counts as (
    select coin_id,
        count(*) as raw_rows
    from {{source('raw', 'daily_prices')}}
    group by coin_id
),
mart_counts as (
    select coin_id,
        count(*) as mart_rows
    from {{ref('mart_daily_summary')}}
    group by coin_id
)
select r.coin_id,
    r.raw_rows,
    m.mart_rows,
    m.mart_rows - r.raw_rows as excess_rows
from raw_counts r
    inner join mart_counts m on r.coin_id = m.coin_id
where m.mart_rows > r.raw_rows