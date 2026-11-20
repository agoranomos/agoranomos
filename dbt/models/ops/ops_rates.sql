-- The rates of the core into the reporting currency, one row for each source of the rates
-- (ADR-0042), which ./agoranomos status reads too, and the loads of the rates
-- (agoranomos_pipelines/rates.py). The sources are those of the order of AGORANOMOS_RATES_SOURCE
-- (rates_chain) whose rates the core holds, and the first of them, also before its first rate, as
-- on a deployment of v0.5 whose first load of the Bank of Italy has not come yet; or, without a
-- rate of any of them, the demo, once the core holds one of it; and each source whose currencies
-- lack a rate; or one row without a source.
-- * The age of the last rate that the source published into the reporting currency, as the core
--   holds it, of a currency or of the second leg of a rate, with a warning after 3 business days,
--   as ops_loads gives the hours since each load: none while the core holds no such rate. The
--   business days are Monday to Friday after the day of the last rate, up to the day on which the
--   view is read, in the time zone of the warehouse: the Bank of Italy and the ECB publish on the
--   working days of TARGET, and their loads of the morning take the days up to yesterday, so the
--   rate of a day is 1 business day old on the next one, and more than 3 is late; the NBK
--   publishes on every calendar day, its rate of a day before the day. A closing day of TARGET
--   counts as a business day.
-- * The days without a rate: for each currency of an account but the reporting currency, the
--   calendar days from the first to the last day of the facts of its accounts on which the core
--   holds no rate of the currency into the reporting currency, as after a change of
--   AGORANOMOS_REPORTING_CURRENCY to a currency whose rates of the days that the core has accepted
--   it cannot take from its own rows (core_fx_rates_daily). The first of them and how many there
--   are, of all the currencies on every row, and of the currencies of the source of the row: the
--   first source of the order, or the demo, that the core holds a rate of the currency from, and
--   for a currency without one the first source, or the demo while the core holds a rate of it
--   and none of a source of the order in any reporting currency, as on the demo stack: a load of
--   that source then takes their history again. Each fact gives the first and the last day of an
--   account through its index of the account and the day.
-- * The rates of the file of the deployment, extensions/model/rates.toml (stg_settings__rates): a
--   row of the source deployment while the file has an entry, with the last day that it covers
--   and no age, as a rate that the deployment gives is never late. And the days off the file: of
--   the facts of a currency, those on which the core holds the rate of the file where the file no
--   longer gives it, or gives another, as after an entry is removed or shrinks, and those on
--   which the file gives a rate that the core does not hold, as for a quote without a rate. They
--   count among the days without a rate of the source of their currency, so that its next load
--   takes their history again (core_fx_rates_daily), and not among first_day_without_rate and
--   days_without_rate, as the core holds a rate on them.
{%- set chain = rates_chain() %}
{%- set links = chain + ['demo_fx'] %}
with rates as (
    select *
    from {{ ref('core_fx_rates_daily') }}
    where reporting_currency = {{ reporting_currency() }}
),

used as (
    select rate_source as source, published_on
    from rates
    where rate_source in ('{{ links | join("', '") }}')
    union all
    select cross_source, cross_published_on
    from rates
    where cross_source in ('{{ links | join("', '") }}')
),

ages as (
    select source, max(published_on) as published_on
    from used
    group by source
),

real as (
    select exists (select from ages where source <> 'demo_fx') as found
),

-- The sources of the rows: the demo only without a rate of a source of the order.
listed as (
    select source
    from ages
    where source <> 'demo_fx' or not (select found from real)
    union
    select '{{ chain[0] }}'
    where (select found from real)
),

-- The source that serves a currency without a rate into the reporting currency: the first of the
-- order, or the demo while the core holds a rate of it and none of a source of the order, in any
-- reporting currency. A change of the reporting currency leaves the core without a rate into the
-- new one, and the source of the deployment takes their history again. rate_source has no index,
-- and the scan of each EXISTS ends at its first match.
serving as (
    select
        case
            when
                exists (
                    select from {{ ref('core_fx_rates_daily') }}
                    where rate_source = 'demo_fx'
                )
                and not exists (
                    select from {{ ref('core_fx_rates_daily') }}
                    where rate_source in ('{{ chain | join("', '") }}')
                )
                then 'demo_fx'
            else '{{ chain[0] }}'
        end as source
),

aged as (
    select
        a.source,
        a.published_on,
        (
            select count(*)
            from generate_series(
                cast(a.published_on + 1 as timestamp), cast(current_date as timestamp),
                interval '1 day'
            ) as d
            where extract(isodow from d) < 6
        ) as business_days
    from ages as a
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

-- The source of each such currency: the first in the order of those of the rows that the core
-- holds a rate of it from, or else the source that serves a currency without a rate.
spans_by_source as (
    select
        s.currency,
        s.first_day,
        s.last_day,
        coalesce(n.source, (select source from serving)) as source
    from spans as s
    left join lateral (
        select r.rate_source as source
        from (select distinct rate_source from rates where currency = s.currency) as r
        where r.rate_source in (select source from listed)
        order by case r.rate_source
            {%- for code in links %}
            when '{{ code }}' then {{ loop.index }}
            {%- endfor %}
        end
        limit 1
    ) as n on true
),

-- The entries of the file of the deployment, of the currencies of the facts, within their days.
given as (
    select
        g.currency::text as currency,
        g.quote_currency::text as quote_currency,
        g.rate,
        greatest(g.first_day, s.first_day) as first_day,
        least(g.last_day, s.last_day) as last_day
    from {{ ref('stg_settings__rates') }} as g
    join spans as s on s.currency = g.currency
    where g.first_day <= s.last_day
        and g.last_day >= s.first_day
),

-- Each day of the facts of each currency without a rate, or off the file: a rate of the file that
-- the file no longer gives as the core holds it, or a rate of the file that the core does not hold.
missing as (
    select s.source, d.day, r.rate_date is not null as off_the_file
    from spans_by_source as s
    cross join lateral (
        select cast(day as date) as day
        from generate_series(
            cast(s.first_day as timestamp), cast(s.last_day as timestamp), interval '1 day'
        ) as day
    ) as d
    left join rates as r
        on r.rate_date = d.day and r.currency = s.currency
    left join given as g
        on g.currency = s.currency and d.day between g.first_day and g.last_day
    where r.rate_date is null
        or (
            (r.rate_source = 'deployment' or g.currency is not null)
            and not (
                r.rate_source = 'deployment'
                and g.currency is not null
                and r.base_currency = g.quote_currency
                and r.base_rate = g.rate
            )
        )
),

gaps as (
    select min(day) as first_day, count(distinct day) as days
    from missing
    where not off_the_file
),

off_the_file as (
    select min(day) as first_day, count(distinct day) as days
    from missing
    where off_the_file
),

gaps_by_source as (
    select source, min(day) as first_day, count(distinct day) as days
    from missing
    group by source
),

-- The rows: the sources listed and those whose currencies lack a rate, or one without a source;
-- and the file of the deployment while it has an entry.
rows as (
    select source from listed
    union
    select source from gaps_by_source
    union all
    select cast(null as text)
    where not exists (select from listed) and not exists (select from gaps_by_source)
    union all
    select 'deployment'
    where exists (select from {{ ref('stg_settings__rates') }})
)

select
    cast({{ reporting_currency() }} as char(3)) as reporting_currency,
    o.source::text as rate_source,
    case
        when o.source = 'deployment'
            then (select max(last_day) from {{ ref('stg_settings__rates') }})
        else a.published_on
    end as last_rate_date,
    a.business_days::integer as business_days_since_rate,
    case when o.source = 'deployment' then false else a.business_days > 3 end as is_late,
    g.first_day as first_day_without_rate,
    g.days::integer as days_without_rate,
    s.first_day as first_day_without_rate_of_source,
    coalesce(s.days, 0)::integer as days_without_rate_of_source,
    f.first_day as first_day_off_the_file,
    f.days::integer as days_off_the_file
from rows as o
cross join gaps as g
cross join off_the_file as f
left join aged as a on a.source = o.source
left join gaps_by_source as s on s.source = coalesce(o.source, (select source from serving))
order by o.source is distinct from (select source from serving), o.source
