-- Singular test: each coin in stg_daily_prices must have at most 90 days.
--
-- CoinGecko's `days=90` parameter sometimes returns more than 90 calendar
-- days. The staging model trims to 90 days, but if that filter breaks or
-- the API behaviour changes, downstream rolling metrics silently cover a
-- wider window than intended - making "30-day volatility" or "7-day avg"
-- not mean what the documentation says.
--
-- Returns rows = FAIL (coin has too many days). Returns nothing = PASS.
select coin_id,
    count(distinct price_date) as actual_days
from {{ref('stg_daily_prices')}}
group by coin_id
having count(distinct price_date) > 90
