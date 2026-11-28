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
-- * The days without a rate of an account in quarantine with missing_rate (macros/quarantine.sql),
--   as of a currency new to the core whose window starts before that of the last load of the
--   rates, whose earlier loads the core has accepted and the raw layer no longer holds: the days
--   of its loads that wait for the core before the first rate of its currency that the core
--   holds from a source, on which it holds no row of it, the days that the check holds out. They
--   count among the days without a rate of the source that the core holds the currency from, so
--   that its next load takes the history again, and the run that reads it takes the account out
--   of quarantine; not among first_day_without_rate and days_without_rate, which are those of the
--   facts. Two guards keep a load from taking the history again and again: a currency of which
--   the core holds no rate names no day, as no source of the order published it on a day that a
--   run read; and a day that a load of the history of that source has covered since the first of
--   those loads was pinned names none, as the source has no rate of it.
-- * The rates of the file of the deployment, extensions/model/rates.toml (int_rates__given): a
--   row of the source deployment while the file has an entry, with the last day that it covers
--   and no age, as a rate that the deployment gives is never late. And the days off the file: of
--   the facts of a currency, those on which the core holds the rate of the file where the file no
--   longer gives it, or gives another, as after an entry is removed or shrinks, and those on
--   which the file gives a rate that the core does not hold, as for a quote without a rate. They
--   count among the days without a rate of the source of their currency, so that its next load
--   takes their history again (core_fx_rates_daily), and not among first_day_without_rate and
--   days_without_rate, as the core holds a rate on them.
-- The view reads only relations that a run of the transform keeps: the tables of the core, the
-- table of the checks (int_g1_failures), the rates of the file (int_rates__given) and the
-- currencies of the listed accounts (int_accounts__currencies), tables that each run fills anew
-- in place, so that it stays through a run, which ./agoranomos status, the Ops collection and the
-- loads of the rates read at any time (agoranomos_pipelines/rates.py). Over a view of staging, it
-- went with that view early in each run, as dbt drops the old view with cascade, until its own
-- node built it again near the end.
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
    from {{ ref('int_rates__given') }} as g
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

-- The loads that wait for the core, pinned and not accepted, of each account that the last run
-- of the transform held in quarantine for days without a rate (int_g1_failures, missing_rate),
-- with the currency that its list gives (int_accounts__currencies): no fact of the core holds
-- their days.
held_loads as (
    select l.currency, p.window_from, p.window_to, p.pinned_at
    from (
        select distinct source, account_id
        from {{ ref('int_g1_failures') }}
        where reason = 'missing_rate'
    ) as q
    join {{ ref('int_accounts__currencies') }} as l
        on l.source = q.source and l.account_id = q.account_id
    join {{ source('core', 'loads') }} as p
        on p.source = q.source and p.account_id = q.account_id
    where p.status = 'pinned'
        and p.outcome = 'loaded'
        and l.currency is not null
        and l.currency <> {{ reporting_currency() }}
),

-- The source of each such currency, the first in the order of those of the rows that the core
-- holds a rate of it from, and when the first of those loads was pinned. The core keeps the rates
-- of the currencies of the accounts that a run reads, those in quarantine too
-- (core_fx_rates_daily), so a currency of which it holds no rate is one that no source of the
-- order published on a day that a run read: it names no day, and no load takes the history for
-- it. Its account stays in quarantine, as for a currency that the source lacks (ADR-0042).
held_sources as (
    select h.currency, h.since, n.source, f.first_rate
    from (
        select currency, min(pinned_at) as since
        from held_loads
        group by currency
    ) as h
    join lateral (
        select r.rate_source as source
        from (select distinct rate_source from rates where currency = h.currency) as r
        where r.rate_source in (select source from listed)
        order by case r.rate_source
            {%- for code in links %}
            when '{{ code }}' then {{ loop.index }}
            {%- endfor %}
        end
        limit 1
    ) as n on true
    -- The first day on which the core holds a rate of the currency from a source of the order,
    -- before which the check of the rates holds the account out (macros/quarantine.sql).
    cross join lateral (
        select min(r.rate_date) as first_rate
        from rates as r
        where r.currency = h.currency
            and r.rate_source in (select source from listed)
    ) as f
),

-- Each day of those loads before the first rate of their currency that the core holds from a
-- source, on which it holds no row of it, the days for which the check of the rates holds the
-- account out, and which no load of the history of its source has covered since the first of the
-- loads was pinned: such a load ended in a run that read the list of the account, and so wrote
-- each rate of the currency that the source published on its days. A day before the first rate
-- that the source published of the currency so asks for the history once, and not with every load
-- of the source. A day after that first rate, as the partial day of today of a load of the kind
-- intraday beyond the last day of the table, names none: the model carries the rate to it, and a
-- load of the history, which ends at its usual last day, would never cover it. They count among
-- the days without a rate of the source, and not among first_day_without_rate and
-- days_without_rate, which are those of the facts: the account is in quarantine, and Ops says why.
held_missing as (
    select s.source, d.day
    from held_loads as h
    join held_sources as s on s.currency = h.currency
    cross join lateral (
        select cast(day as date) as day
        from generate_series(
            cast(h.window_from as timestamp), cast(h.window_to as timestamp), interval '1 day'
        ) as day
    ) as d
    where d.day < s.first_rate
        and not exists (
            select from rates as r where r.rate_date = d.day and r.currency = h.currency
        )
        and not exists (
            select
            from {{ source('core', 'loads') }} as a
            where a.source = s.source
                and a.kind = 'history'
                and a.status = 'accepted'
                and a.pinned_at >= s.since
                and d.day between a.window_from and a.window_to
        )
),

gaps_by_source as (
    select source, min(day) as first_day, count(distinct day) as days
    from (
        select source, day from missing
        union all
        select source, day from held_missing
    ) as without_rate
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
    where exists (select from {{ ref('int_rates__given') }})
)

select
    cast({{ reporting_currency() }} as char(3)) as reporting_currency,
    o.source::text as rate_source,
    case
        when o.source = 'deployment'
            then (select max(last_day) from {{ ref('int_rates__given') }})
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
