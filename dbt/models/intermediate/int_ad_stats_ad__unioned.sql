-- The statistics of every platform by ad and day, with the current names of the campaign, the ad
-- group and the ad, the client and the currency of the account, and the conversions of the primary
-- actions only, from the report by conversion action at the same level (ADR-0034). A day on which
-- an ad has primary conversions and no other metric keeps them, with zeros for the rest.
-- A table, unlike the other intermediate models: each of its many tests reads it, and as a view each
-- would compute the statistics of the ads again, two tests at once more than the memory of
-- warehouse-db holds.
{{ config(materialized='table') }}
-- A test checks the ads of each ad group against the ad group, so it runs after both.
-- depends_on: {{ ref('int_ad_stats_ad_group__unioned') }}
with stats as (
    select * from {{ ref('stg_demo_search__ad_stats') }}
    union all
    select * from {{ ref('stg_demo_social__ad_stats') }}
    union all
    select * from {{ ref('stg_demo_video__ad_stats') }}
    union all
    select * from {{ ref('stg_google_ads__ad_stats') }}
),

conversions as (
    select * from {{ ref('stg_demo_search__ad_conversions') }}
    union all
    select * from {{ ref('stg_demo_social__ad_conversions') }}
    union all
    select * from {{ ref('stg_demo_video__ad_conversions') }}
    union all
    select * from {{ ref('stg_google_ads__ad_conversions') }}
),

primary_conversions as (
    select
        c.report_date,
        c.platform,
        c.account_id,
        c.campaign_id,
        c.ad_group_id,
        c.ad_id,
        sum(c.conversions) as conversions,
        sum(c.conversion_value) as conversion_value
    from conversions as c
    join {{ ref('int_conversion_actions__resolved') }} as r
        on r.platform = c.platform and r.conversion_action_id = c.conversion_action_id
    where r.is_primary
    group by c.report_date, c.platform, c.account_id, c.campaign_id, c.ad_group_id, c.ad_id
),

keyed as (
    select report_date, platform, account_id, campaign_id, ad_group_id, ad_id from stats
    union
    select report_date, platform, account_id, campaign_id, ad_group_id, ad_id
    from primary_conversions
)

select
    k.report_date,
    k.platform,
    a.client_id,
    a.client_name,
    k.account_id,
    a.account_name,
    k.campaign_id,
    m.campaign_name,
    k.ad_group_id,
    g.ad_group_name,
    k.ad_id,
    coalesce(d.ad_name, nullif(s.loaded_ad_name, ''), k.ad_id) as ad_name,
    a.currency,
    coalesce(s.impressions, 0) as impressions,
    coalesce(s.clicks, 0) as clicks,
    coalesce(s.spend, 0) as spend,
    coalesce(p.conversions, 0) as conversions,
    coalesce(p.conversion_value, 0) as conversion_value
from keyed as k
left join stats as s
    on s.report_date = k.report_date and s.platform = k.platform
    and s.account_id = k.account_id and s.ad_group_id = k.ad_group_id and s.ad_id = k.ad_id
left join primary_conversions as p
    on p.report_date = k.report_date and p.platform = k.platform
    and p.account_id = k.account_id and p.ad_group_id = k.ad_group_id and p.ad_id = k.ad_id
left join {{ ref('int_ad_accounts__unioned') }} as a
    on a.platform = k.platform and a.account_id = k.account_id
left join {{ ref('int_ad_campaigns__unioned') }} as m
    on m.platform = k.platform and m.account_id = k.account_id and m.campaign_id = k.campaign_id
left join {{ ref('int_ad_groups__unioned') }} as g
    on g.platform = k.platform and g.account_id = k.account_id and g.ad_group_id = k.ad_group_id
left join {{ ref('int_ads__unioned') }} as d
    on d.platform = k.platform and d.account_id = k.account_id
    and d.ad_group_id = k.ad_group_id and d.ad_id = k.ad_id
