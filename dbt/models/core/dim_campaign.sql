-- core.dim_campaign, the campaigns with their current attributes (ADR-0036): one row for each
-- campaign of int_campaigns__current, the newest list of its account, the core for one that the
-- list no longer names, or, for a stub, its newest row of statistics, with its ID as bigint and as
-- text. The model merges into the table of the migrations (dbt_project.yml), and only the rows
-- that are new or that changed.
-- * name_count counts the names of the versions of the campaign in the core
--   (dim_campaign_versions), which the snapshot of this run has just taken from the same view, so
--   that the current name and a name from the statistics of a stub count too. The versions start
--   at the first run that saw the campaign, so it counts the names that the runs saw.
-- * last_seen_at and the first and the last day of statistics only move forward or widen: this run
--   reads the loads that the core has not accepted (ADR-0035), and a list that no longer names a
--   campaign leaves the moment when one last did.
-- * No connector gives the dates or the budget of a campaign yet, so they are none.
{{ config(unique_key=['account_sk', 'campaign_id']) }}
with existing as (
    select * from {{ this }}
),

counted as (
    select account_sk, campaign_id, count(distinct campaign_name)::integer as name_count
    from {{ ref('dim_campaign_versions') }}
    group by account_sk, campaign_id
),

merged as (
    select
        c.account_sk::integer as account_sk,
        c.campaign_id::bigint as campaign_id,
        c.campaign_id::bigint::text as campaign_id_text,
        c.current_name::text as campaign_name,
        c.campaign_status::text as campaign_status,
        c.campaign_state::text as campaign_state,
        c.campaign_type::text as campaign_type,
        cast(null as date) as start_date,
        cast(null as date) as end_date,
        cast(null as numeric(18,6)) as budget,
        cast(null as text) as budget_period,
        n.name_count,
        least(d.first_report_date, c.first_report_date) as first_report_date,
        greatest(d.last_report_date, c.last_report_date) as last_report_date,
        greatest(d.last_seen_at, c.last_seen_at) as last_seen_at,
        c.is_listed
    from {{ ref('int_campaigns__current') }} as c
    left join existing as d
        on d.account_sk = c.account_sk and d.campaign_id = c.campaign_id::bigint
    left join counted as n
        on n.account_sk = c.account_sk and n.campaign_id = c.campaign_id::bigint
)

select m.*
from merged as m
left join existing as d
    on d.account_sk = m.account_sk and d.campaign_id = m.campaign_id
where d.campaign_id is null
    or (
        m.campaign_name, m.campaign_status, m.campaign_state, m.campaign_type, m.start_date,
        m.end_date, m.budget, m.budget_period, m.name_count, m.first_report_date,
        m.last_report_date, m.last_seen_at, m.is_listed
    ) is distinct from (
        d.campaign_name, d.campaign_status, d.campaign_state, d.campaign_type, d.start_date,
        d.end_date, d.budget, d.budget_period, d.name_count, d.first_report_date,
        d.last_report_date, d.last_seen_at, d.is_listed
    )
