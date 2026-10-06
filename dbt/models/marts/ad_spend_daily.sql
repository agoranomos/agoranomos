-- Advertising spend and results by day and campaign. Metabase reads the marts layer only.
select
    report_date,
    account_id,
    campaign_id,
    campaign_name,
    currency,
    impressions,
    clicks,
    spend,
    conversions
from {{ ref('stg_demo__ad_stats_daily') }}
