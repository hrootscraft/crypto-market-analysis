-- Mart: exchange market share
-- Computes each exchange's share of total 24h BTC volume.
-- Tracks how major exchanges compete for trading volume over time.
--
-- Business context: an exchange's take rate (revenue / volume) depends
-- on maintaining or growing market share. Tracking this daily helps
-- identify competitive dynamics and inform pricing strategy.
-- Grain: one row per exchange per snapshot date
with daily_totals as (
    select snapshot_date,
        sum(volume_btc_24h) as total_volume_btc
    from {{ref('stg_exchange_volumes')}}
    group by 1
)
select e.exchange_id,
    e.exchange_name,
    e.snapshot_date,
    e.volume_btc_24h,
    e.trust_score,
    dt.total_volume_btc,
    -- Market share as percentage
    round(
        100.0 * e.volume_btc_24h / nullif(dt.total_volume_btc, 0),
        4
    ) as market_share_pct,
    -- Rank by volume
    rank() over (
        partition by e.snapshot_date
        order by e.volume_btc_24h desc
    ) as volume_rank
from {{ref('stg_exchange_volumes')}} e
    inner join daily_totals dt on e.snapshot_date = dt.snapshot_date