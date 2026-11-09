-- A click needs an impression, so a row with more clicks than impressions is suspect. A warning:
-- some platforms count clicks on more than the ads that they count as shown. It reads the days of
-- the windows of the run.
{{ config(severity='warn') }}
select report_date, platform, account_id, campaign_id
from {{ ref('ad_stats_campaign_daily') }}
where clicks > impressions and report_date >= {{ window_start() }}
