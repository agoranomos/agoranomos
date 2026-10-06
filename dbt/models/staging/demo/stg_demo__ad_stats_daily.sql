-- The demo statistics with the column names of the marts layer, one row per campaign and day.
select
    "date" as report_date,
    account_id,
    campaign_id,
    campaign_name,
    currency,
    impressions,
    clicks,
    spend,
    conversions
from {{ source('raw_demo', 'ad_stats_daily') }}
