-- Singular test: no date gaps in mart_daily_summary.
--
-- Date gaps silently corrupt rolling window functions. A "7-day" rolling
-- average computed across a missing date covers fewer than 7 actual days
-- with no error raised — volatility and return metrics are just quietly wrong.
--
-- Grain: one row per coin per gap found.
-- Returns rows = FAIL. Returns nothing = PASS.
select coin_id,
    min(price_date) as first_date,
    max(price_date) as last_date,
    count(distinct price_date) as actual_days,
    (max(price_date) - min(price_date) + 1) - count(distinct price_date) as missing_days
from {{ref('mart_daily_summary')}}
group by coin_id
having (max(price_date) - min(price_date) + 1) - count(distinct price_date) > 0