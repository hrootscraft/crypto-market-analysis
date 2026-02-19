-- Intermediate: flag days where trading volume is anomalous
-- A volume anomaly = volume exceeds 2 standard deviations from 30-day mean
-- Useful for identifying market events, whale activity, or exchange outages
-- Grain: one row per coin per day (all days with sufficient volume history)
with base as (
    select coin_id,
        price_date,
        price_usd,
        volume_usd,
        daily_return,
        avg_volume_30d,
        stddev_volume_30d
    from {{ref('int_rolling_volatility')}}
    where avg_volume_30d is not null
        and stddev_volume_30d is not null
        and stddev_volume_30d > 0
)
select coin_id,
    price_date,
    price_usd,
    volume_usd,
    daily_return,
    avg_volume_30d,
    stddev_volume_30d,
    -- Z-score: how many standard deviations from the 30-day mean
    (volume_usd - avg_volume_30d) / stddev_volume_30d as volume_z_score,
    -- Classification
    case
        when volume_usd > avg_volume_30d + 2 * stddev_volume_30d then 'high_anomaly'
        when volume_usd < avg_volume_30d - 2 * stddev_volume_30d then 'low_anomaly'
        else 'normal'
    end as volume_flag
from base