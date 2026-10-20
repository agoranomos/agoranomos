-- A click needs an impression, so a row with more clicks than impressions is suspect. A warning:
-- some platforms count clicks on more than the ads that they count as shown.
{{ config(severity='warn') }}
select report_date, platform, account_id, campaign_id
from {{ ref('int_ad_stats__unioned') }}
where clicks > impressions
