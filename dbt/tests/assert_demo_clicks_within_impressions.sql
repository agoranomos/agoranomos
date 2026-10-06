-- A click needs an impression, so a row with more clicks than impressions is broken data.
select report_date, account_id, campaign_id
from {{ ref('stg_demo__ad_stats_daily') }}
where clicks > impressions
