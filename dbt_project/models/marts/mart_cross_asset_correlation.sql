-- Mart: rolling 30-day Pearson correlation between BTC and each altcoin
-- Tracks how closely altcoins move with Bitcoin over time.
-- When correlation approaches 1.0, diversification benefit disappears.
-- Grain: one row per (non-BTC) coin per day
with returns as (
    select coin_id,
        price_date,
        daily_return
    from {{ref('int_rolling_volatility')}}
    where daily_return is not null
),
btc_returns as (
    select price_date,
        daily_return as btc_return
    from returns
    where coin_id = 'bitcoin'
),
paired as (
    select r.coin_id,
        r.price_date,
        r.daily_return as alt_return,
        b.btc_return
    from returns r
        inner join btc_returns b on r.price_date = b.price_date
    where r.coin_id != 'bitcoin'
),
with_correlation as (
    select coin_id,
        price_date,
        alt_return,
        btc_return,
        -- 30-day rolling Pearson correlation (computed once)
        corr(btc_return, alt_return) over (
            partition by coin_id
            order by price_date rows between 29 preceding and current row
        ) as correlation_30d
    from paired
)
select coin_id,
    price_date,
    alt_return,
    btc_return,
    correlation_30d,
    case
        when correlation_30d >= 0.8 then 'high_correlation'
        when correlation_30d >= 0.4 then 'moderate_correlation'
        when correlation_30d >= 0 then 'low_correlation'
        else 'negative_correlation'
    end as correlation_regime
from with_correlation
