-- The conversions of the primary actions of a campaign on a day, conversions_primary of the view of
-- campaigns, are the sum of its primary actions in the view of the conversions by action: a
-- difference means that a join doubled or lost rows. A warning, as one can be right: a day on
-- which a campaign has nothing but the conversions of a primary action that the own metric of
-- the platform leaves out, which Google can report, has no row of statistics (ADR-0036), so the
-- view of campaigns does not show them. It reads the days of the windows of the run.
{{ config(severity='warn') }}
with by_action as (
    select
        report_date,
        platform,
        account_id,
        campaign_id,
        sum(coalesce(all_conversions, conversions)) as conversions
    from {{ ref('ad_conversions_campaign_daily') }}
    where is_primary and report_date >= {{ window_start() }}
    group by report_date, platform, account_id, campaign_id
),

by_campaign as (
    select report_date, platform, account_id, campaign_id, conversions_primary
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
