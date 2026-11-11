-- Advertising statistics by day and ad on every platform, a view over the core (ADR-0037): one
-- row of core.ad_stats_ad_daily, with the names and states of its dimensions, money in the
-- currency of the account and in the reporting currency at the rate of its day, and when its load
-- ended, as ad_stats_campaign_daily has them. An ad without a name on its platform, such as a
-- responsive search ad of Google, shows its ID. The conversions of the primary actions are none
-- at this level: the conversions by action of ads are a module (ADR-0036).
-- The test of the ads against their ad groups reads the view of ad groups, which dbt orders before
-- it: depends_on: {{ ref('ad_stats_ad_group_daily') }}
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
    g.ad_group_id_text as ad_group_id,
    g.ad_group_name,
    d.ad_id_text as ad_id,
    coalesce(d.ad_name, d.ad_id_text) as ad_name,
    cast(f.currency as text) as currency,
    f.impressions,
    f.clicks,
    f.spend,
    f.conversions,
    f.conversion_value,
    cast({{ reporting_currency() }} as text) as reporting_currency,
    cast(f.spend * r.rate_into_reporting_currency as numeric(18,6)) as spend_reporting,
    cast(f.conversion_value * r.rate_into_reporting_currency as numeric(18,6))
        as conversion_value_reporting,
    a.time_zone,
    d.ad_status,
    d.ad_state,
    d.ad_type,
    d.is_listed,
    f.all_conversions,
    cast(null as numeric(18,6)) as conversions_primary,
    r.rate_into_reporting_currency as fx_rate_into_reporting_currency,
    r.published_on as fx_rate_date,
    r.is_carried_forward as is_fx_rate_carried_forward,
    l.finished_at as loaded_at,
    a.is_active as is_account_active
from {{ ref('core_ad_stats_ad_daily') }} as f
left join {{ ref('dim_account') }} as a
    on a.account_sk = f.account_sk
left join {{ ref('dim_client') }} as c
    on c.client_id = a.client_id
left join {{ platforms(ref('seed_ad_platforms')) }}
    on p.platform = a.platform
left join {{ ref('dim_campaign') }} as m
    on m.account_sk = f.account_sk and m.campaign_id = f.campaign_id
left join {{ ref('dim_ad_group') }} as g
    on g.account_sk = f.account_sk and g.ad_group_id = f.ad_group_id
left join {{ ref('dim_ad') }} as d
    on d.account_sk = f.account_sk and d.ad_group_id = f.ad_group_id and d.ad_id = f.ad_id
left join {{ ref('core_fx_rates_daily') }} as r
    on r.rate_date = f.report_date and r.currency = f.currency
    and r.reporting_currency = {{ reporting_currency() }}
left join {{ source('core', 'loads') }} as l
    on l.load_sk = f.load_sk
