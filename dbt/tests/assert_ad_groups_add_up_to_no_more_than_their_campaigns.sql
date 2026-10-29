-- The ad groups of a campaign on a day add up to no more than the campaign: a platform counts each
-- impression, click and cost of an ad group in its campaign too, and a campaign can have more,
-- such as one of Performance Max, which has no ad groups. More means that a join doubled rows. A
-- warning: two reports of a platform loaded at different moments can disagree on recent days, and
-- a platform that rounds money at each level can add up a cent over for each ad group.
{{ config(severity='warn') }}
with ad_groups as (
    select
        report_date,
        platform,
        account_id,
        campaign_id,
        count(*) as ad_groups,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(spend) as spend
    from {{ ref('int_ad_stats_ad_group__unioned') }}
    group by report_date, platform, account_id, campaign_id
)

select g.report_date, g.platform, g.account_id, g.campaign_id
from ad_groups as g
left join {{ ref('int_ad_stats__unioned') }} as c
    on c.report_date = g.report_date and c.platform = g.platform
    and c.account_id = g.account_id and c.campaign_id = g.campaign_id
where g.impressions > coalesce(c.impressions, 0)
    or g.clicks > coalesce(c.clicks, 0)
    or g.spend > coalesce(c.spend, 0) + 0.01 * g.ad_groups
