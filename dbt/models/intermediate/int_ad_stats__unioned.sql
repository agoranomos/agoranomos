-- The statistics of every platform by campaign and day, with the current name of each campaign,
-- the client and the currency of the account, and the conversions of the primary actions. A day
-- on which a campaign has primary conversions and no other metric, which Google can report, keeps
-- them with zeros for the rest.
with stats as (
    select * from {{ ref('stg_demo_search__campaign_stats') }}
    union all
    select * from {{ ref('stg_demo_social__campaign_stats') }}
    union all
    select * from {{ ref('stg_demo_video__campaign_stats') }}
),

primary_conversions as (
    select
        report_date,
        platform,
        account_id,
        campaign_id,
        sum(conversions) as conversions,
        sum(conversion_value) as conversion_value
    from {{ ref('int_ad_conversions__unioned') }}
    where is_primary
    group by report_date, platform, account_id, campaign_id
),

keyed as (
    select report_date, platform, account_id, campaign_id from stats
    union
    select report_date, platform, account_id, campaign_id from primary_conversions
)

select
    k.report_date,
    k.platform,
    a.client_id,
    a.client_name,
    k.account_id,
    a.account_name,
    k.campaign_id,
    coalesce(c.campaign_name, s.loaded_campaign_name) as campaign_name,
    a.currency,
    coalesce(s.impressions, 0) as impressions,
    coalesce(s.clicks, 0) as clicks,
    coalesce(s.spend, 0) as spend,
    coalesce(p.conversions, 0) as conversions,
    coalesce(p.conversion_value, 0) as conversion_value
from keyed as k
left join stats as s
    on s.report_date = k.report_date and s.platform = k.platform
    and s.account_id = k.account_id and s.campaign_id = k.campaign_id
left join primary_conversions as p
    on p.report_date = k.report_date and p.platform = k.platform
    and p.account_id = k.account_id and p.campaign_id = k.campaign_id
left join {{ ref('int_ad_accounts__unioned') }} as a
    on a.platform = k.platform and a.account_id = k.account_id
left join {{ ref('int_ad_campaigns__unioned') }} as c
    on c.platform = k.platform and c.account_id = k.account_id and c.campaign_id = k.campaign_id
