-- The conversions of the primary actions of a campaign on a day, conversions_primary of the view of
-- campaigns, and their value, conversion_value_primary, are the sums of its primary actions in the
-- view of the conversions by action (ADR-0056): a difference means that a join doubled or lost
-- rows. Staging keeps a row of statistics whose measures are all zero when its campaign has a
-- conversion on its day (ADR-0036, nonzero), so a day with nothing but the conversions of a
-- primary action has its row. A warning, as one can still be right: Google Ads gives its
-- statistics and its conversions by action in two reports, and staging keeps only the rows that
-- the report of statistics holds, so a campaign day that is only in the report of conversions has
-- no row of statistics. It reads the days of the windows of the run.
{{ config(severity='warn') }}
with by_action as (
    select
        report_date,
        platform,
        account_id,
        campaign_id,
        sum(coalesce(all_conversions, conversions)) as conversions,
        sum(coalesce(all_conversion_value, conversion_value)) as conversion_value
    from {{ ref('ad_conversions_campaign_daily') }}
    where is_primary and report_date >= {{ window_start() }}
    group by report_date, platform, account_id, campaign_id
),

by_campaign as (
    select
        report_date,
        platform,
        account_id,
        campaign_id,
        conversions_primary,
        conversion_value_primary
    from {{ ref('ad_stats_campaign_daily') }}
    where report_date >= {{ window_start() }}
)

select
    coalesce(s.report_date, a.report_date) as report_date,
    coalesce(s.platform, a.platform) as platform,
    coalesce(s.account_id, a.account_id) as account_id,
    coalesce(s.campaign_id, a.campaign_id) as campaign_id
from by_campaign as s
full join by_action as a
    on a.report_date = s.report_date and a.platform = s.platform
    and a.account_id = s.account_id and a.campaign_id = s.campaign_id
where coalesce(s.conversions_primary, 0) <> coalesce(a.conversions, 0)
    or coalesce(s.conversion_value_primary, 0) <> coalesce(a.conversion_value, 0)
