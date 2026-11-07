-- core.dim_ad_group, the ad groups with their campaign and their current attributes (ADR-0036): one
-- row for each ad group of int_ad_groups__current, the newest list of its account or, for a stub,
-- its newest row of statistics, with its ID as bigint and as text. The model merges into the table
-- of the migrations (dbt_project.yml), and only the rows that are new or that changed.
-- last_seen_at and the first and the last day of statistics only move forward or widen, as in
-- core.dim_campaign. No connector gives the dates or the budget of an ad group yet, so they are
-- none.
{{ config(unique_key=['account_sk', 'ad_group_id']) }}
with existing as (
    select * from {{ this }}
),

merged as (
    select
        g.account_sk::integer as account_sk,
        g.ad_group_id::bigint as ad_group_id,
        g.ad_group_id::bigint::text as ad_group_id_text,
        g.campaign_id::bigint as campaign_id,
        g.current_name::text as ad_group_name,
        g.ad_group_status::text as ad_group_status,
        g.ad_group_state::text as ad_group_state,
        g.ad_group_type::text as ad_group_type,
        cast(null as date) as start_date,
        cast(null as date) as end_date,
        cast(null as numeric(18,6)) as budget,
        cast(null as text) as budget_period,
        least(d.first_report_date, g.first_report_date) as first_report_date,
        greatest(d.last_report_date, g.last_report_date) as last_report_date,
        greatest(d.last_seen_at, g.last_seen_at) as last_seen_at,
        g.is_listed
    from {{ ref('int_ad_groups__current') }} as g
    left join existing as d
        on d.account_sk = g.account_sk and d.ad_group_id = g.ad_group_id::bigint
)

select m.*
from merged as m
left join existing as d
    on d.account_sk = m.account_sk and d.ad_group_id = m.ad_group_id
where d.ad_group_id is null
    or (
        m.campaign_id, m.ad_group_name, m.ad_group_status, m.ad_group_state, m.ad_group_type,
        m.start_date, m.end_date, m.budget, m.budget_period, m.first_report_date,
        m.last_report_date, m.last_seen_at, m.is_listed
    ) is distinct from (
        d.campaign_id, d.ad_group_name, d.ad_group_status, d.ad_group_state, d.ad_group_type,
        d.start_date, d.end_date, d.budget, d.budget_period, d.first_report_date,
        d.last_report_date, d.last_seen_at, d.is_listed
    )
