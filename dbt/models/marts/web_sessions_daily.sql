-- Sessions of web analytics by day, property, source, medium and campaign, linked to the ad
-- campaign whose ID the links carry, with revenue in the reporting currency.
select
    w.report_date,
    w.platform,
    p.platform_name,
    w.client_id,
    w.client_name,
    w.property_id,
    w.property_name,
    w.session_source,
    w.session_medium,
    w.session_campaign_id,
    w.session_campaign_name,
    w.session_default_channel_group,
    w.ad_platform,
    w.ad_account_id,
    w.currency,
    w.sessions,
    w.engaged_sessions,
    cast(w.key_events as numeric(18,6)) as key_events,
    cast(w.total_revenue as numeric(18,6)) as total_revenue,
    f.reporting_currency,
    cast(round(w.total_revenue * f.rate, 6) as numeric(18,6)) as total_revenue_reporting
from {{ ref('int_web_sessions__unioned') }} as w
join {{ ref('int_ad_platforms__unioned') }} as p on p.platform = w.platform
join {{ ref('int_fx_rates__filled') }} as f on f.rate_date = w.report_date and f.currency = w.currency
