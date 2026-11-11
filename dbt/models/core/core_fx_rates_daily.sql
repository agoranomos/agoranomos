-- core.fx_rates_daily, the rates into the reporting currency (ADR-0042, ADR-0037): one row for each
-- calendar day, each currency of the accounts and the reporting currency, with the last rate that
-- the source published on or before the day, through its base currency with no rounding
-- (macros/rates.sql). The model merges into the table of the migrations (dbt_project.yml), and only
-- the rows that are new or that changed, so a run that brings no new rate writes no row.
-- * The days that a run computes: those of the windows of the loads of the rates that it reads,
--   each of which takes the last 14 days again (ADR-0042), so that a rate that the source publishes
--   late, or corrects, replaces a carried one; every day after the last that the table holds, to
--   the last day of those windows and of the windows of the facts of the run; and the days of the
--   windows of the facts that the table does not hold for the currency of their account, as for
--   the history of a new account. A run computes every day when the table holds no row of the
--   reporting currency, as on the first run or after a change of AGORANOMOS_REPORTING_CURRENCY, and
--   when the var recompute_rates is true.
-- * The rates that a day takes: those that staging reads, on the days of the windows of the loads
--   of the rates, and those that the table holds as published, on the other days, as the raw layer
--   keeps only the loads that the core has not accepted (ADR-0035). A day without a
--   publication takes the last rate published before it, with is_carried_forward; a currency has
--   no row before the first rate that the source published of it.
-- * The reporting currency has the rate 1 on every day, whether or not a source publishes rates.
{{ config(
    alias='fx_rates_daily',
    unique_key=['rate_date', 'currency', 'reporting_currency'],
) }}
with existing as (
    select *
    from {{ this }}
    where reporting_currency = {{ reporting_currency() }}
),

published as (
    {{ rates_published(ref('stg_ecb__rates_daily'), ref('stg_demo_fx__rates_daily'), this) }}
),

-- The days of the windows of the loads of the source of the rates that this run reads: staging
-- holds every rate of these days, and a day among them without one has no publication.
read_days as (
    select distinct day
    from {{ ref('stg_loads__newest_days') }}
    where source = {{ rates_source(ref('stg_ecb__rates_daily'), this) }}
),

window_days as (
    select report_date, currency::text as currency
    from {{ ref('int_window_days') }}
),

currencies as (
    select distinct currency::text as currency
    from {{ ref('dim_account') }}
    where currency is not null
    union
    select {{ reporting_currency() }}
),

bounds as (
    select
        case
            when {{ 'true' if var('recompute_rates') else 'false' }}
                or not exists (select from existing)
                then least(
                    (select min(day) from read_days),
                    (select min(report_date) from window_days),
                    (select min(rate_date) from {{ this }})
                )
            else least(
                (select min(day) from read_days),
                (select max(rate_date) + 1 from existing),
                (
                    select min(w.report_date)
                    from window_days as w
                    where not exists (
                        select
                        from existing as e
                        where e.rate_date = w.report_date and e.currency = w.currency
                    )
                )
            )
        end as first_day,
        greatest(
            (select max(day) from read_days),
            (select max(report_date) from window_days),
            (select max(rate_date) from existing)
        ) as last_day
),

days as (
    select cast(day as date) as rate_date
    from bounds
    cross join lateral generate_series(
        cast(bounds.first_day as timestamp), cast(bounds.last_day as timestamp), interval '1 day'
    ) as day
),

-- The rates known on their days of publication: those of the loads that this run reads, and those
-- that the table holds as published on the days that it does not read, from the same source.
read_rates as (
    {{ rates_into_reporting('published') }}
),

held_rates as (
    select
        e.published_on,
        e.currency::text as currency,
        e.base_currency::text as base_currency,
        e.base_rate,
        e.rate_into_reporting_currency,
        e.rate_source
    from existing as e
    where not e.is_carried_forward
        and e.rate_source = {{ rates_source(ref('stg_ecb__rates_daily'), this) }}
        and not exists (select from read_days as d where d.day = e.rate_date)
),

known as (
    select published_on, currency, base_currency, base_rate, rate_into_reporting_currency, rate_source
    from read_rates
    union all
    select published_on, currency, base_currency, base_rate, rate_into_reporting_currency, rate_source
    from held_rates
),

-- For each currency, the last rate known before the first day that the run computes, which its
-- first days carry.
before as (
    select distinct on (k.currency)
        k.published_on,
        k.currency,
        k.base_currency,
        k.base_rate,
        k.rate_into_reporting_currency,
        k.rate_source
    from known as k
    where k.published_on < (select first_day from bounds)
    order by k.currency, k.published_on desc
),

-- Each day and currency with the rate published on it, if any, and the last rate before the first
-- day; each run of days from a publication to the day before the next takes the rate of its first
-- day, in a window, where a join to the last rate on or before each day would compare every day
-- with every rate.
series as (
    select
        d.rate_date,
        c.currency,
        k.published_on,
        k.base_currency,
        k.base_rate,
        k.rate_into_reporting_currency,
        k.rate_source
    from days as d
    cross join currencies as c
    left join known as k
        on k.currency = c.currency and k.published_on = d.rate_date
    where c.currency <> {{ reporting_currency() }}
    union all
    select
        published_on,
        currency,
        published_on,
        base_currency,
        base_rate,
        rate_into_reporting_currency,
        rate_source
    from before
),

runs as (
    select
        *,
        count(published_on) over (partition by currency order by rate_date) as publications
    from series
),

carried as (
    select
        rate_date,
        currency,
        first_value(published_on) over run_of_days as published_on,
        first_value(base_currency) over run_of_days as base_currency,
        first_value(base_rate) over run_of_days as base_rate,
        first_value(rate_into_reporting_currency) over run_of_days as rate_into_reporting_currency,
        first_value(rate_source) over run_of_days as rate_source
    from runs
    window run_of_days as (partition by currency, publications order by rate_date)
),

computed as (
    select
        rate_date,
        {{ reporting_currency() }} as currency,
        cast(1 as numeric) as rate_into_reporting_currency,
        cast(null as text) as base_currency,
        cast(null as numeric) as base_rate,
        'identity' as rate_source,
        cast(null as date) as published_on,
        false as is_carried_forward
    from days
    union all
    select
        rate_date,
        currency,
        rate_into_reporting_currency,
        base_currency,
        base_rate,
        rate_source,
        published_on,
        published_on < rate_date as is_carried_forward
    from carried
    where published_on is not null
        and rate_date >= (select first_day from bounds)
),

typed as (
    select
        cast(rate_date as date) as rate_date,
        cast(currency as char(3)) as currency,
        cast({{ reporting_currency() }} as char(3)) as reporting_currency,
        cast(rate_into_reporting_currency as numeric) as rate_into_reporting_currency,
        cast(base_currency as char(3)) as base_currency,
        cast(base_rate as numeric(20,10)) as base_rate,
        cast(rate_source as text) as rate_source,
        cast(published_on as date) as published_on,
        cast(is_carried_forward as boolean) as is_carried_forward
    from computed
)

select t.*
from typed as t
left join existing as e
    on e.rate_date = t.rate_date and e.currency = t.currency
where e.rate_date is null
    or (
        t.rate_into_reporting_currency, t.base_currency, t.base_rate, t.rate_source,
        t.published_on, t.is_carried_forward
    ) is distinct from (
        e.rate_into_reporting_currency, e.base_currency, e.base_rate, e.rate_source,
        e.published_on, e.is_carried_forward
    )
