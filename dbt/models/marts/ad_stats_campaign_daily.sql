-- Advertising statistics by day and campaign on every platform, a view over the core (ADR-0037):
-- one row of core.ad_stats_campaign_daily, with the names and states of its dimensions, money in
-- the currency of the account and in the reporting currency at the rate of its day, and when its
-- load ended. A plain join, so that PostgreSQL carries a filter of a question down to the index of
-- the fact; each dimension joins on its unique key with LEFT JOIN, so that the planner leaves out
-- those that a question does not read. The IDs are the text of the dimensions, as a cast of the
-- fact would keep a filter on them from its index. The conversions of the primary actions are a
-- LEFT JOIN LATERAL to the conversions by action of the row: one lookup in the key of that fact
-- for each row that a question reads, however often the question names the column, and none for
-- a question that does not read it, as the planner leaves out the join of an aggregate of one
-- row. A subquery in the select list ran once for each mention, and a join to the conversions
-- grouped by the key of the row would sum the whole history for every question, as PostgreSQL
-- carries no filter of a period across a join (B16). The primary actions are an array that the
-- query reads once, so that the lookup is a condition of the index. The columns of v0.4 come
-- first, in their order, and the later ones after them. Metabase reads the marts layer only.
-- The test of the primary actions reads the view of the conversions by action, which dbt orders
-- before it: depends_on: {{ ref('ad_conversions_campaign_daily') }}
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
    m.campaign_status,
    m.campaign_state,
    m.campaign_type,
    m.is_listed,
    f.all_conversions,
    cast(coalesce(q.conversions_primary, 0) as numeric(18,6)) as conversions_primary,
    r.rate_into_reporting_currency as fx_rate_into_reporting_currency,
    r.published_on as fx_rate_date,
    r.is_carried_forward as is_fx_rate_carried_forward,
    l.finished_at as loaded_at,
    a.is_active as is_account_active
from {{ ref('core_ad_stats_campaign_daily') }} as f
left join {{ ref('dim_account') }} as a
    on a.account_sk = f.account_sk
left join {{ ref('dim_client') }} as c
    on c.client_id = a.client_id
left join {{ platforms(ref('seed_ad_platforms')) }}
    on p.platform = a.platform
left join {{ ref('dim_campaign') }} as m
    on m.account_sk = f.account_sk and m.campaign_id = f.campaign_id
left join {{ ref('core_fx_rates_daily') }} as r
    on r.rate_date = f.report_date and r.currency = f.currency
    and r.reporting_currency = {{ reporting_currency() }}
left join {{ source('core', 'loads') }} as l
    on l.load_sk = f.load_sk
left join lateral (
    select sum(coalesce(x.all_conversions, x.conversions)) as conversions_primary
    from {{ ref('core_ad_conversions_campaign_daily') }} as x
    where x.account_sk = f.account_sk
        and x.campaign_id = f.campaign_id
        and x.report_date = f.report_date
        and x.conversion_action_sk = any(array(
            select k.conversion_action_sk
            from {{ ref('dim_conversion_action') }} as k
            where k.is_primary
        ))
) as q
    on true
