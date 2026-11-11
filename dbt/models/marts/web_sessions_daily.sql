-- Sessions of web analytics by day, property, source, medium and campaign, a view over the core
-- (ADR-0037): one row of core.web_sessions_daily, with the property and its client, the ad
-- campaign whose ID the links carried in utm_id when exactly one campaign of the core has it,
-- revenue in the currency of the property and in the reporting currency at the rate of its day,
-- and when its load ended. The campaigns come down to one row for each ID before the join, which
-- the planner leaves out for a question that does not read them.
select
    f.report_date,
    a.platform,
    p.platform_name,
    a.client_id,
    c.client_name,
    a.account_id as property_id,
    a.account_name as property_name,
    f.session_source,
    f.session_medium,
    f.session_campaign_id,
    f.session_campaign_name,
    f.session_default_channel_group,
    ad.platform as ad_platform,
    ad.account_id as ad_account_id,
    cast(f.currency as text) as currency,
    f.sessions,
    f.engaged_sessions,
    f.key_events,
    f.total_revenue,
    cast({{ reporting_currency() }} as text) as reporting_currency,
    cast(f.total_revenue * r.rate_into_reporting_currency as numeric(18,6))
        as total_revenue_reporting,
    a.time_zone,
    r.rate_into_reporting_currency as fx_rate_into_reporting_currency,
    r.published_on as fx_rate_date,
    r.is_carried_forward as is_fx_rate_carried_forward,
    l.finished_at as loaded_at,
    a.is_active as is_account_active
from {{ ref('core_web_sessions_daily') }} as f
left join {{ ref('dim_account') }} as a
    on a.account_sk = f.account_sk
left join {{ ref('dim_client') }} as c
    on c.client_id = a.client_id
left join {{ platforms(ref('seed_ad_platforms')) }}
    on p.platform = a.platform
left join (
    select
        m.campaign_id_text collate "C" as campaign_id,
        min(d.platform) as platform,
        min(d.account_id) as account_id
    from {{ ref('dim_campaign') }} as m
    join {{ ref('dim_account') }} as d
        on d.account_sk = m.account_sk
    group by m.campaign_id_text collate "C"
    having count(*) = 1
) as ad
    on ad.campaign_id = f.session_campaign_id
left join {{ ref('core_fx_rates_daily') }} as r
    on r.rate_date = f.report_date and r.currency = f.currency
    and r.reporting_currency = {{ reporting_currency() }}
left join {{ source('core', 'loads') }} as l
    on l.load_sk = f.load_sk
