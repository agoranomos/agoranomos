-- Conversions by day, campaign and conversion action on every platform, a view over the core
-- (ADR-0037): one row of core.ad_conversions_campaign_daily, with the names of its dimensions, the
-- category of its action and whether the action is primary, money in the currency of the account
-- and in the reporting currency at the rate of its day, and when its load ended, as
-- ad_stats_campaign_daily has them.
select
    f.report_date,
    a.platform,
    p.platform_name,
    a.client_id,
    c.client_name,
    a.account_id,
    a.account_name,
    m.campaign_id_text as campaign_id,
    m.campaign_name,
    k.conversion_action_id,
    k.conversion_action_name,
    k.conversion_category,
    k.is_primary,
    f.attribution_setting,
    cast(f.currency as text) as currency,
    f.conversions,
    f.conversion_value,
    cast({{ reporting_currency() }} as text) as reporting_currency,
    cast(f.conversion_value * r.rate_into_reporting_currency as numeric(18,6))
        as conversion_value_reporting,
    a.time_zone,
    f.all_conversions,
    f.all_conversion_value,
    r.rate_into_reporting_currency as fx_rate_into_reporting_currency,
    r.published_on as fx_rate_date,
    r.is_carried_forward as is_fx_rate_carried_forward,
    l.finished_at as loaded_at,
    a.is_active as is_account_active
from {{ ref('core_ad_conversions_campaign_daily') }} as f
left join {{ ref('dim_account') }} as a
    on a.account_sk = f.account_sk
left join {{ ref('dim_client') }} as c
    on c.client_id = a.client_id
left join {{ platforms(ref('seed_ad_platforms')) }}
    on p.platform = a.platform
left join {{ ref('dim_campaign') }} as m
    on m.account_sk = f.account_sk and m.campaign_id = f.campaign_id
left join {{ ref('dim_conversion_action') }} as k
    on k.conversion_action_sk = f.conversion_action_sk
left join {{ ref('core_fx_rates_daily') }} as r
    on r.rate_date = f.report_date and r.currency = f.currency
    and r.reporting_currency = {{ reporting_currency() }}
left join {{ source('core', 'loads') }} as l
    on l.load_sk = f.load_sk
