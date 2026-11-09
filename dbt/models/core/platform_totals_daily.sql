-- core.platform_totals_daily, the totals of each account by day as its platform gives them in a
-- request of their own, against which int_g1_failures checked the rows of its campaigns before
-- this write (G0a of ADR-0035). For each account and day of the windows that this run writes,
-- int_window_days, which leaves out the accounts in quarantine, the totals of the newest load of
-- the day, and zero on a day of the window of which the load holds no totals, as the platform
-- gives no row for a day without a metric; the model merges them into the table of the migrations
-- by its key (dbt_project.yml), so a day of a window is replaced and the days before it stay. A
-- load that holds no totals at all, as one of a connector before the totals came, writes nothing.
-- Today only the connector of Google Ads loads totals; the demo has none.
{{ config(unique_key=['source', 'account_id', 'report_date']) }}
with totals as (
    select
        platform as source,
        account_id,
        report_date,
        load_id,
        sum(total_impressions) as impressions,
        sum(total_clicks) as clicks,
        sum(total_spend) as spend
    from {{ ref('stg_google_ads__account_totals') }}
    group by platform, account_id, report_date, load_id
),

totalled as (
    select distinct source, load_id
    from totals
)

select
    d.source::text as source,
    d.account_id::text as account_id,
    d.report_date::date as report_date,
    coalesce(t.impressions, 0)::bigint as impressions,
    coalesce(t.clicks, 0)::bigint as clicks,
    coalesce(t.spend, 0)::numeric(18,6) as spend,
    d.currency::char(3) as currency,
    d.load_id::text as load_id
from {{ ref('int_window_days') }} as d
join totalled as w
    on w.source = d.source and w.load_id = d.load_id
left join totals as t
    on t.source = d.source
    and t.account_id = d.account_id
    and t.report_date = d.report_date
    and t.load_id = d.load_id
