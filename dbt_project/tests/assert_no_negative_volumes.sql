-- Custom data quality test: no negative volumes should ever exist
-- in the daily summary. A negative volume indicates a data quality
-- issue upstream (API error or parsing bug).
select coin_id,
    price_date,
    volume_usd
from {{ref('mart_daily_summary')}}
where volume_usd < 0