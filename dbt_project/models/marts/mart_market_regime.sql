-- Mart: market regime classification
-- Classifies each coin-day into a market regime based on 
-- moving average crossover + volatility level
-- 
-- Regime logic (simplified trend-following heuristic):
--   BULL:     7d avg return > 0 AND 30d avg return > 0
--   BEAR:     7d avg return < 0 AND 30d avg return < 0
--   SIDEWAYS: mixed signals or low volatility
--
-- This is a standard approach for classifying market conditions
-- in crypto analytics; useful for contextualizing product metrics.
-- Grain: one row per coin per day
with base as (
    select coin_id,
        price_date,
        price_usd,
        volume_usd,
        daily_return,
        avg_return_7d,
        avg_return_30d,
        volatility_7d,
        volatility_30d
    from {{ref('int_rolling_volatility')}}
    where avg_return_7d is not null and avg_return_30d is not null
)
select coin_id,
    price_date,
    price_usd,
    volume_usd,
    daily_return,
    avg_return_7d,
    avg_return_30d,
    volatility_7d,
    volatility_30d,
    case
        when avg_return_7d > 0
        and avg_return_30d > 0 then 'bull'
        when avg_return_7d < 0
        and avg_return_30d < 0 then 'bear'
        else 'sideways'
    end as market_regime,
    case
        when volatility_30d > 0.05 then 'high_vol'
        when volatility_30d > 0.02 then 'medium_vol'
        else 'low_vol'
    end as volatility_regime
from base