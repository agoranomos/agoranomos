-- The ads of an ad group on a day add up to no more than the ad group, as its ad groups do to a
-- campaign (assert_ad_groups_add_up_to_no_more_than_their_campaigns). A warning, for the same
-- reasons.
{{ config(severity='warn') }}
with ads as (
    select
        report_date,
        platform,
        account_id,
        ad_group_id,
        count(*) as ads,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(spend) as spend
    from {{ ref('int_ad_stats_ad__unioned') }}
    group by report_date, platform, account_id, ad_group_id
)

select a.report_date, a.platform, a.account_id, a.ad_group_id
from ads as a
left join {{ ref('int_ad_stats_ad_group__unioned') }} as g
    on g.report_date = a.report_date and g.platform = a.platform
    and g.account_id = a.account_id and g.ad_group_id = a.ad_group_id
where a.impressions > coalesce(g.impressions, 0)
    or a.clicks > coalesce(g.clicks, 0)
    or a.spend > coalesce(g.spend, 0) + 0.01 * a.ads
