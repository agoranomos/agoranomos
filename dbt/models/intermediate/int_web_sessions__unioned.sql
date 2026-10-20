-- The sessions of web analytics by day, property, source, medium and campaign, with the client of
-- the property, and the ad campaign whose ID the links carried in utm_id, when exactly one
-- campaign of the data has it.
with mapped as (
    select
        platform,
        account_id,
        client_id,
        row_number() over (
            partition by platform, account_id order by case origin when 'extensions' then 0 else 1 end
        ) as preference
    from {{ ref('stg_settings__account_clients') }}
),

campaigns as (
    select campaign_id, min(platform) as platform, min(account_id) as account_id, count(*) as matches
    from {{ ref('int_ad_campaigns__unioned') }}
    group by campaign_id
)

select
    s.report_date,
    s.platform,
    c.client_id,
    c.client_name,
    s.property_id,
    p.property_name,
    s.session_source,
    s.session_medium,
    s.session_campaign_id,
    s.session_campaign_name,
    s.session_default_channel_group,
    case when ad.matches = 1 then ad.platform end as ad_platform,
    case when ad.matches = 1 then ad.account_id end as ad_account_id,
    p.currency,
    s.sessions,
    s.engaged_sessions,
    s.key_events,
    s.total_revenue
from {{ ref('stg_demo_web__sessions_daily') }} as s
join {{ ref('stg_demo_web__properties') }} as p
    on p.platform = s.platform and p.property_id = s.property_id
left join mapped as m
    on m.platform = s.platform and m.account_id = s.property_id and m.preference = 1
join {{ ref('int_clients__resolved') }} as c
    on c.client_id = coalesce(m.client_id, 'unassigned')
left join campaigns as ad
    on ad.campaign_id = s.session_campaign_id
