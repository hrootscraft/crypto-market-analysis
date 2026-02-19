-- Mart: daily market summary
-- A single table optimized for dashboard consumption.
-- Combines price, regime, and anomaly data for all coins.
-- Grain: one row per coin per day
with base as (
    select r.coin_id,
        r.price_date,
        r.price_usd,
        r.volume_usd,
        r.daily_return,
        r.market_regime,
        r.volatility_regime,
        r.volatility_30d,
        -- Volume anomaly flag (join from anomaly model)
        coalesce(a.volume_flag, 'normal') as volume_flag,
        a.volume_z_score,
        -- Running max price (for drawdown calculation)
        max(r.price_usd) over (
            partition by r.coin_id
            order by r.price_date rows between unbounded preceding and current row
        ) as running_max_price,
        -- Drawdown from peak
        round(
            100.0 * (
                r.price_usd - max(r.price_usd) over (
                    partition by r.coin_id
                    order by r.price_date rows between unbounded preceding and current row
                )
            ) / nullif(
                max(r.price_usd) over (
                    partition by r.coin_id
                    order by r.price_date rows between unbounded preceding and current row
                ),
                0
            ),
            2
        ) as drawdown_pct
    from {{ref('mart_market_regime')}} r
        left join {{ref('int_volume_anomalies')}} a on r.coin_id = a.coin_id
        and r.price_date = a.price_date
)
select *,
    -- Severity bucket derived from drawdown_pct for dashboard color-coding
    case
        when drawdown_pct < -30 then 'critical'
        when drawdown_pct < -15 then 'moderate'
        else 'mild'
    end as drawdown_severity
from base
