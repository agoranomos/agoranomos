-- The rates of the core into the reporting currency, in one row (ADR-0042), which ./agoranomos
-- status reads too, and the loads of the rates (agoranomos_pipelines/rates.py):
-- * The age of the last rate that the source of the rates published into the reporting currency,
--   as the core holds it, with a warning after 3 business days, as ops_loads gives the hours since
--   each load: none while the core holds no such rate. The business days are Monday to Friday
--   after the day of the last rate, up to the day on which the view is read, in the time zone of
--   the warehouse: the ECB publishes on the working days of TARGET, and its load of the morning
--   takes the days up to yesterday, so the rate of a day is 1 business day old on the next one, and
--   more than 3 is late. A closing day of TARGET counts as a business day.
-- * The days without a rate: for each currency of an account but the reporting currency, the
--   calendar days from the first to the last day of the facts of its accounts on which the core
--   holds no rate of the currency into the reporting currency, as after a change of
--   AGORANOMOS_REPORTING_CURRENCY to a currency whose rates of the days that the core has accepted
--   it cannot take from its own rows (core_fx_rates_daily). The first of them, and how many there
--   are: a load of the source of the rates then takes their history again. Each fact gives the
--   first and the last day of an account through its index of the account and the day.
with newest as (
    select rate_source, published_on
    from {{ ref('core_fx_rates_daily') }}
    where reporting_currency = {{ reporting_currency() }}
        and rate_source <> 'identity'
    order by published_on desc, rate_source
    limit 1
),

aged as (
    select
        n.rate_source,
        n.published_on,
        (
            select count(*)
            from generate_series(
                cast(n.published_on + 1 as timestamp), cast(current_date as timestamp),
                interval '1 day'
            ) as d
            where extract(isodow from d) < 6
        ) as business_days
    from newest as n
),

accounts as (
    select a.currency, f.first_day, f.last_day
    from {{ ref('dim_account') }} as a
    cross join lateral (
        select min(s.first_day) as first_day, max(s.last_day) as last_day
        from (
            select min(report_date) as first_day, max(report_date) as last_day
            from {{ ref('core_ad_stats_campaign_daily') }}
            where account_sk = a.account_sk
            union all
            select min(report_date), max(report_date)
            from {{ ref('core_ad_stats_ad_group_daily') }}
            where account_sk = a.account_sk
            union all
            select min(report_date), max(report_date)
            from {{ ref('core_ad_stats_ad_daily') }}
            where account_sk = a.account_sk
            union all
            select min(report_date), max(report_date)
            from {{ ref('core_ad_conversions_campaign_daily') }}
            where account_sk = a.account_sk
            union all
            select min(report_date), max(report_date)
            from {{ ref('core_web_sessions_daily') }}
            where account_sk = a.account_sk
        ) as s
    ) as f
    where a.currency is not null
        and a.currency <> {{ reporting_currency() }}
        and f.first_day is not null
),

spans as (
    select currency, min(first_day) as first_day, max(last_day) as last_day
    from accounts
    group by currency
),

missing as (
    select cast(d.day as date) as day
    from spans as s
    cross join lateral generate_series(
        cast(s.first_day as timestamp), cast(s.last_day as timestamp), interval '1 day'
    ) as d (day)
    where not exists (
        select
        from {{ ref('core_fx_rates_daily') }} as r
        where r.rate_date = cast(d.day as date)
            and r.currency = s.currency
            and r.reporting_currency = {{ reporting_currency() }}
    )
),

gaps as (
    select min(day) as first_day, count(distinct day) as days
    from missing
)

select
    cast({{ reporting_currency() }} as char(3)) as reporting_currency,
    a.rate_source::text as rate_source,
    a.published_on as last_rate_date,
    a.business_days::integer as business_days_since_rate,
    a.business_days > 3 as is_late,
    g.first_day as first_day_without_rate,
    g.days::integer as days_without_rate
from gaps as g
left join aged as a on true
