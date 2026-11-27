-- core.fx_rates_daily, the rates into the reporting currency (ADR-0042, ADR-0037): one row for each
-- calendar day, each currency of the accounts and the reporting currency, with the last rate that
-- the sources published on or before the day, in the order that AGORANOMOS_REPORTING_CURRENCY and
-- AGORANOMOS_RATES_SOURCE give, through the base currency of the first source with no rounding
-- (macros/rates.sql). The model merges into the table of the migrations (dbt_project.yml), and
-- only the rows that are new or that changed, so a run that brings no new rate writes no row.
-- * The currencies of the accounts: those of core.dim_account, and those of the accounts that the
--   lists of this run give, also of an account in quarantine. A currency new to the core whose
--   account goes to quarantine with missing_rate for the days before its first rate in this run
--   (macros/quarantine.sql) so gets the rates that the run reads of it, which the raw layer loses
--   once the core accepts their loads, and ops_rates knows from which source to take the history
--   of the days before (agoranomos_pipelines/rates.py); a currency that no source publishes gets
--   no row.
-- * The days that a run computes: those of the windows of the loads of the rates that it reads,
--   each of which takes the last 14 days again (ADR-0042), so that a rate that a source publishes
--   late, or corrects, replaces a carried one; every day after the last that the table holds, to
--   the last day of those windows and of the windows of the facts of the run; and the days of the
--   windows of the facts that the table does not hold for the currency of their account, as for
--   the history of a new account. A run computes every day when the table holds no row of the
--   reporting currency, as on the first run or after a change of AGORANOMOS_REPORTING_CURRENCY, and
--   when the var recompute_rates is true.
-- * The rates that a day takes: those that staging reads, on the days of the windows of the loads
--   of the rates, and those that the table holds as published, on the other days, as the raw layer
--   keeps only the loads that the core has not accepted (ADR-0035), each source in its place in
--   the order (rates_known). A day without a publication takes the last rate published before
--   it, with is_carried_forward; a currency has no row before the first rate published of it.
-- * The reporting currency has the rate 1 on every day, whether or not a source publishes rates.
-- * The rates of the file of the deployment, extensions/model/rates.toml (stg_settings__rates), go
--   before every source on each day of their entries, for a currency of the accounts but the
--   reporting currency: rate_source deployment, base_currency the quote of the entry, base_rate
--   its rate, published_on its first day, and never carried forward, as the rate holds for the
--   whole range by agreement. A quote in the reporting currency is the rate itself; another quote
--   goes through its own rate into the reporting currency, the last that the order of the
--   sources gives on or before the day against the base of the first source, in one division,
--   and the row keeps that rate as its second leg (cross_source, cross_rate, cross_published_on).
--   The file is small, so each run computes every day of its entries from the first day of the
--   table, or of what the run reads, to the last day that the run computes, and a changed rate
--   reaches each day of its entry. A day whose second leg no rate gives keeps the row that the
--   table holds, and ops_rates names it.
-- * A day that the file no longer covers keeps the rate of the file until a load of its source
--   brings its rate again: the raw layer no longer holds the rate of the source for a day that
--   the core has accepted, and the rates of the sources do not carry over a day of the file
--   (marks). ops_rates counts such a day off the file, among the days without a rate of its
--   source, so the next load of the source takes the history of the rates again from it
--   (agoranomos_pipelines/rates.py), and the run that reads that load writes the rate of the
--   source.
{{ config(
    alias='fx_rates_daily',
    unique_key=['rate_date', 'currency', 'reporting_currency'],
) }}
{%- set staged = {
    'bank_of_italy': ref('stg_bank_of_italy__rates_daily'),
    'nbk': ref('stg_nbk__rates_daily'),
    'ecb': ref('stg_ecb__rates_daily'),
    'demo_fx': ref('stg_demo_fx__rates_daily'),
} %}
{%- set lists = [
    (ref('stg_demo_search__accounts'), 'account_id'),
    (ref('stg_demo_social__accounts'), 'account_id'),
    (ref('stg_demo_video__accounts'), 'account_id'),
    (ref('stg_google_ads__accounts'), 'account_id'),
    (ref('stg_meta_ads__accounts'), 'account_id'),
    (ref('stg_tiktok_ads__accounts'), 'account_id'),
    (ref('stg_demo_web__properties'), 'property_id'),
    (ref('stg_ga4__properties'), 'property_id'),
] %}
with existing as (
    select *
    from {{ this }}
    where reporting_currency = {{ reporting_currency() }}
),

-- The days of the windows of the loads of the sources of the rates that this run reads: staging
-- holds every rate of these days, and a day among them without one has no publication.
read_days as (
    select distinct day
    from {{ ref('stg_loads__newest_days') }}
    where source in ('{{ (rates_chain() + ['demo_fx']) | join("', '") }}')
),

window_days as (
    select report_date, currency::text as currency
    from {{ ref('int_window_days') }}
),

-- The currencies of the registry of accounts, those of the accounts that the lists of this run
-- give, also of one in quarantine, and the reporting currency.
currencies as (
    select distinct currency::text as currency
    from {{ ref('dim_account') }}
    where currency is not null
    union
    select distinct currency::text
    from ({{ listed_currencies(lists) }}) as l
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

-- The rates known on their days of publication, of the loads that this run reads and of the
-- table on the days that it does not read, each currency from the first source of the order
-- that has it (rates_known), and the quotes of the file of the deployment for its second legs.
known as (
    {{ rates_known(staged, this, ref('stg_loads__newest_days'), source('core', 'loads'), 'select currency from currencies union select quote_currency from ' ~ ref('stg_settings__rates')) }}
),

-- The rates known of the currencies of the accounts, and a mark without a rate on each day whose
-- rate of the sources the run cannot know: a day on which the table holds the rate of the file of
-- the deployment, which keeps no rate of a source, and which no load that the run reads covers.
-- The rate of an earlier day does not carry over a mark, so such a day, and the days after it
-- without a publication, keep the rows that the table holds.
marks as (
    select
        published_on,
        currency,
        base_currency,
        base_rate,
        rate_into_reporting_currency,
        rate_source,
        cross_source,
        cross_rate,
        cross_published_on
    from known
    where currency in (select currency from currencies)
    union all
    select
        e.rate_date,
        e.currency::text,
        cast(null as text),
        cast(null as numeric),
        cast(null as numeric),
        cast(null as text),
        cast(null as text),
        cast(null as numeric),
        cast(null as date)
    from existing as e
    where e.rate_source = 'deployment'
        and not exists (select from read_days as r where r.day = e.rate_date)
        and not exists (
            select from known as k where k.currency = e.currency and k.published_on = e.rate_date
        )
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
        k.rate_source,
        k.cross_source,
        k.cross_rate,
        k.cross_published_on
    from marks as k
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
        k.rate_source,
        k.cross_source,
        k.cross_rate,
        k.cross_published_on
    from days as d
    cross join currencies as c
    left join marks as k
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
        rate_source,
        cross_source,
        cross_rate,
        cross_published_on
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
        first_value(rate_source) over run_of_days as rate_source,
        first_value(cross_source) over run_of_days as cross_source,
        first_value(cross_rate) over run_of_days as cross_rate,
        first_value(cross_published_on) over run_of_days as cross_published_on
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
        false as is_carried_forward,
        cast(null as text) as cross_source,
        cast(null as numeric) as cross_rate,
        cast(null as date) as cross_published_on
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
        published_on < rate_date as is_carried_forward,
        cross_source,
        cross_rate,
        cross_published_on
    from carried
    where published_on is not null
        and rate_into_reporting_currency is not null
        and rate_date >= (select first_day from bounds)
),

-- The days of the entries of the file of the deployment that the run computes, for the currencies
-- of the accounts but the reporting currency: from the first day of the table, or of what the run
-- reads, to the last day that it computes.
given_days as (
    select
        cast(d.day as date) as rate_date,
        f.currency::text as currency,
        f.quote_currency::text as quote_currency,
        f.rate,
        f.first_day
    from {{ ref('stg_settings__rates') }} as f
    cross join lateral generate_series(
        cast(greatest(f.first_day, least(
            (select min(day) from read_days),
            (select min(report_date) from window_days),
            (select min(rate_date) from existing)
        )) as timestamp),
        cast(least(f.last_day, (select last_day from bounds)) as timestamp),
        interval '1 day'
    ) as d (day)
    where f.currency in (select currency from currencies)
        and f.currency <> {{ reporting_currency() }}
        and (select last_day from bounds) is not null
),

-- The days on which each source published, as this run knows them: from what staging reads and
-- the table holds of it, as a rate of a currency or as the second leg of one.
publication_days as (
    select rate_source as source, published_on as day
    from known
    union
    select rate_source, published_on
    from {{ this }}
    where not is_carried_forward
        and rate_source not in ('identity', 'deployment')
    union
    select cross_source, cross_published_on
    from {{ this }}
    where cross_source is not null
),

-- The rates of the file: in the reporting currency as they are; in another quote, through the
-- last rate of the quote into the reporting currency that the order of the sources gives on or
-- before the day against the base of the first source, its second leg, in one division. That
-- rate counts only when its source published nothing after it up to the day: on a day whose loads
-- the core has accepted, the run may know an older rate of the quote and not the newest, as when
-- the core holds the rate of the base from the file and so none of the reporting currency
-- (rates_known), and the row keeps what the table holds.
given as (
    select
        d.rate_date,
        d.currency,
        case
            when d.quote_currency = {{ reporting_currency() }} then d.rate
            else d.rate * l.rate_numerator / l.rate_denominator
        end as rate_into_reporting_currency,
        d.quote_currency as base_currency,
        d.rate as base_rate,
        'deployment' as rate_source,
        d.first_day as published_on,
        false as is_carried_forward,
        l.rate_source as cross_source,
        l.base_rate as cross_rate,
        l.published_on as cross_published_on
    from given_days as d
    left join lateral (
        select k.rate_source, k.base_rate, k.published_on, k.rate_numerator, k.rate_denominator
        from known as k
        where d.quote_currency <> {{ reporting_currency() }}
            and k.currency = d.quote_currency
            and k.cross_source is null
            and k.published_on <= d.rate_date
        order by k.published_on desc
        limit 1
    ) as l on true
    where d.quote_currency = {{ reporting_currency() }}
        or (
            l.rate_source is not null
            and not exists (
                select
                from publication_days as p
                where p.source = l.rate_source
                    and p.day > l.published_on
                    and p.day <= d.rate_date
            )
        )
),

-- Each day and currency with the rate of the file where it gives one, and else with the rate of
-- the sources.
merged as (
    select
        rate_date,
        currency,
        rate_into_reporting_currency,
        base_currency,
        base_rate,
        rate_source,
        published_on,
        is_carried_forward,
        cross_source,
        cross_rate,
        cross_published_on
    from given
    union all
    select
        c.rate_date,
        c.currency,
        c.rate_into_reporting_currency,
        c.base_currency,
        c.base_rate,
        c.rate_source,
        c.published_on,
        c.is_carried_forward,
        c.cross_source,
        c.cross_rate,
        c.cross_published_on
    from computed as c
    where not exists (
        select from given as g where g.rate_date = c.rate_date and g.currency = c.currency
    )
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
        cast(is_carried_forward as boolean) as is_carried_forward,
        cast(cross_source as text) as cross_source,
        cast(cross_rate as numeric(20,10)) as cross_rate,
        cast(cross_published_on as date) as cross_published_on
    from merged
)

select t.*
from typed as t
left join existing as e
    on e.rate_date = t.rate_date and e.currency = t.currency
where e.rate_date is null
    or (
        t.rate_into_reporting_currency, t.base_currency, t.base_rate, t.rate_source,
        t.published_on, t.is_carried_forward, t.cross_source, t.cross_rate, t.cross_published_on
    ) is distinct from (
        e.rate_into_reporting_currency, e.base_currency, e.base_rate, e.rate_source,
        e.published_on, e.is_carried_forward, e.cross_source, e.cross_rate, e.cross_published_on
    )
