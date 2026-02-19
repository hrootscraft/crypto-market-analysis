-- Singular test: market_share_pct must sum to ~100% per snapshot date.
--
-- market_share_pct is computed as each exchange's volume divided by the
-- total volume across all exchanges for that day. The sum across all
-- exchanges must equal exactly 100%. Any deviation means the denominator
-- in mart_exchange_market_share is wrong - percentages are not comparable
-- across dates and competitive share figures cannot be trusted.
--
-- Tolerance: [99.9, 100.1] to absorb floating-point rounding.
-- Returns rows = FAIL. Returns nothing = PASS.
select snapshot_date,
    round(sum(market_share_pct)::numeric, 4) as total_share_pct,
    count(*) as exchange_count
from {{ref('mart_exchange_market_share')}}
group by snapshot_date
having round(sum(market_share_pct)::numeric, 4) not between 99.9 and 100.1