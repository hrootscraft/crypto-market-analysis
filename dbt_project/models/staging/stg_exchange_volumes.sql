-- Staging layer: get the latest exchange volume data for each exchange, filtering out any rows with invalid volumes
-- Source: raw.exchange_volumes (CoinGecko API)
-- Grain: one row per exchange per snapshot date
with source as (
    select exchange_id,
        exchange_name,
        volume_btc_24h,
        trust_score,
        snapshot_date,
        ingested_at,
        row_number() over (
            partition by exchange_id,
            snapshot_date
            order by ingested_at desc
        ) as row_num
    from {{source('raw', 'exchange_volumes')}}
    where volume_btc_24h > 0
)
select exchange_id,
    exchange_name,
    volume_btc_24h,
    trust_score,
    snapshot_date
from source
where row_num = 1