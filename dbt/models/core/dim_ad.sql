-- core.dim_ad, the ads with their campaign, their ad group and their current attributes
-- (ADR-0036): one row for each ad of int_ads__current, the newest list of its account or, for a
-- stub, its newest row of statistics, with its ID as bigint and as text; an ad is defined only
-- inside its ad group, which is part of its key. The model merges into the table of the migrations
-- (dbt_project.yml), and only the rows that are new or that changed. last_seen_at and the first
-- and the last day of statistics only move forward or widen, as in core.dim_campaign.
{{ config(unique_key=['account_sk', 'ad_group_id', 'ad_id']) }}
with existing as (
    select * from {{ this }}
),

merged as (
    select
        e.account_sk::integer as account_sk,
        e.ad_group_id::bigint as ad_group_id,
        e.ad_id::bigint as ad_id,
        e.ad_id::bigint::text as ad_id_text,
        e.campaign_id::bigint as campaign_id,
        e.current_name::text as ad_name,
        e.ad_status::text as ad_status,
        e.ad_state::text as ad_state,
        e.ad_type::text as ad_type,
        least(d.first_report_date, e.first_report_date) as first_report_date,
        greatest(d.last_report_date, e.last_report_date) as last_report_date,
        greatest(d.last_seen_at, e.last_seen_at) as last_seen_at,
        e.is_listed
    from {{ ref('int_ads__current') }} as e
    left join existing as d
        on d.account_sk = e.account_sk and d.ad_group_id = e.ad_group_id::bigint
        and d.ad_id = e.ad_id::bigint
)

select m.*
from merged as m
left join existing as d
    on d.account_sk = m.account_sk and d.ad_group_id = m.ad_group_id and d.ad_id = m.ad_id
where d.ad_id is null
    or (
        m.campaign_id, m.ad_name, m.ad_status, m.ad_state, m.ad_type, m.first_report_date,
        m.last_report_date, m.last_seen_at, m.is_listed
    ) is distinct from (
        d.campaign_id, d.ad_name, d.ad_status, d.ad_state, d.ad_type, d.first_report_date,
        d.last_report_date, d.last_seen_at, d.is_listed
    )
