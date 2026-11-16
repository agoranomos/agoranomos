-- How much the platforms change the days that a load replaces, by platform, kind of the load and age
-- of the day (ADR-0036, "The journal of restatements"), so that the window of each platform can be
-- set from data (ADR-0038, "The window"). It reads the journal of the campaigns alone,
-- core.ad_stats_campaign_daily_restatements, which is enough to see when a day stops changing and
-- is the smallest, over the replacements of the last 35 days, so that PostgreSQL reads one or two
-- monthly partitions of it, and the fact of the campaigns only on the days of those windows.
-- * The days that a load replaced: each day of the window of a load that the core accepted, of an
--   ad platform, that a load accepted before it had taken, and on which the fact holds rows of the
--   account now or the journal names a row that the load replaced. A day that no load before had
--   taken is new, such as yesterday in a daily load, and a load that the same run read beside a
--   newer one of the account replaced none of the days of the newer one (int_window_days).
-- * The age of a day: the day on which its load ended, in the time zone of the account, minus the
--   day: 1 for yesterday.
-- * The new value of a row of the journal: the old value of the next row of the journal with the
--   same key, as a replacement that changed nothing writes no row, or else the row of the fact as
--   it is now, or 0 for a row that vanished. The journal holds no row that a load added to a day.
-- * The shift of a day: the change of its total, new minus old, divided by its total now, averaged
--   over the days replaced; a day whose total is 0 now is left out of the average. Money of several
--   currencies is not summed, but for the change of spend in the reporting currency, at the rate of
--   the day. A window function is allowed here: the rule of none is for the marts (ADR-0037).
-- Partial rows of a day (#223) never enter the journal, so nothing here leaves them out.
with loads as (
    select
        l.load_sk,
        l.source,
        l.account_id,
        l.kind,
        l.load_id,
        l.window_from,
        l.window_to,
        l.finished_at,
        l.accepted_at,
        min(l.window_from) over earlier as covered_from,
        max(l.window_to) over earlier as covered_to
    from {{ source('core', 'loads') }} as l
    where l.status = 'accepted'
        and l.outcome = 'loaded'
        and l.source in ('{{ fact_sources("ad_stats_campaign_daily") | join("', '") }}')
    -- The loads that the core accepted before each, in an earlier run.
    window earlier as (
        partition by l.source, l.account_id
        order by l.accepted_at
        range between unbounded preceding and current row exclude group
    )
),

recent as (
    select
        l.load_sk,
        l.source,
        l.account_id,
        l.kind,
        l.load_id,
        l.accepted_at,
        -- None for the first load of an account, as greatest and least leave out a NULL.
        case when l.covered_from is not null then greatest(l.window_from, l.covered_from) end
            as first_day,
        case when l.covered_to is not null then least(l.window_to, l.covered_to) end as last_day,
        a.account_sk,
        (l.finished_at at time zone coalesce(a.time_zone, 'UTC'))::date as load_day
    from loads as l
    join {{ ref('dim_account') }} as a
        on a.platform = l.source and a.account_id = l.account_id
    where l.accepted_at >= now() - interval '35 days'
),

window_days as (
    select
        r.load_sk,
        r.account_sk,
        cast(d.day as date) as report_date,
        row_number() over (
            partition by r.source, r.account_id, r.accepted_at, cast(d.day as date)
            order by r.load_id::numeric desc
        ) as newest
    from recent as r
    cross join lateral generate_series(
        cast(r.first_day as timestamp), cast(r.last_day as timestamp), interval '1 day'
    ) as d (day)
),

journal as (
    select
        j.account_sk,
        j.campaign_id,
        j.report_date,
        j.spend,
        coalesce(j.conversions, 0) as conversions,
        j.currency,
        j.new_load_sk,
        j.is_vanished,
        lead(true, 1, false) over later as has_next,
        lead(j.spend) over later as next_spend,
        lead(coalesce(j.conversions, 0)) over later as next_conversions
    from {{ source('core', 'ad_stats_campaign_daily_restatements') }} as j
    where j.replaced_at >= now() - interval '35 days'
    window later as (partition by j.account_sk, j.campaign_id, j.report_date order by j.replaced_at)
),

changed as (
    select
        j.new_load_sk as load_sk,
        j.account_sk,
        j.report_date,
        j.currency,
        j.is_vanished,
        j.spend,
        j.conversions,
        case
            when j.is_vanished then 0
            when j.has_next then j.next_spend
            else coalesce(f.spend, 0)
        end as new_spend,
        case
            when j.is_vanished then 0
            when j.has_next then j.next_conversions
            else coalesce(f.conversions, 0)
        end as new_conversions
    from journal as j
    join recent as r
        on r.load_sk = j.new_load_sk
    left join {{ ref('core_ad_stats_campaign_daily') }} as f
        on f.account_sk = j.account_sk
        and f.campaign_id = j.campaign_id
        and f.report_date = j.report_date
),

changes as (
    select
        c.load_sk,
        c.account_sk,
        c.report_date,
        count(*) filter (where not c.is_vanished) as changed_rows,
        count(*) filter (where c.is_vanished) as vanished_rows,
        sum(c.new_spend - c.spend) as spend_change,
        sum(c.new_conversions - c.conversions) as conversions_change,
        sum((c.new_spend - c.spend) * x.rate_into_reporting_currency) as spend_change_reporting
    from changed as c
    left join {{ ref('core_fx_rates_daily') }} as x
        on x.rate_date = c.report_date
        and x.currency = c.currency
        and x.reporting_currency = {{ reporting_currency() }}
    group by c.load_sk, c.account_sk, c.report_date
),

replaced as (
    select load_sk, account_sk, report_date
    from window_days
    where newest = 1
    union
    select load_sk, account_sk, report_date
    from changes
),

days as (
    select
        r.source,
        r.kind,
        r.load_day - d.report_date as age_days,
        coalesce(c.changed_rows, 0) as changed_rows,
        coalesce(c.vanished_rows, 0) as vanished_rows,
        coalesce(c.spend_change, 0) as spend_change,
        coalesce(c.conversions_change, 0) as conversions_change,
        c.spend_change_reporting,
        t.rows_now,
        t.spend_now,
        t.conversions_now
    from replaced as d
    join recent as r
        on r.load_sk = d.load_sk
    cross join lateral (
        select
            count(*) as rows_now,
            coalesce(sum(f.spend), 0) as spend_now,
            coalesce(sum(f.conversions), 0) as conversions_now
        from {{ ref('core_ad_stats_campaign_daily') }} as f
        where f.account_sk = d.account_sk
            and f.report_date = d.report_date
    ) as t
    left join changes as c
        on c.load_sk = d.load_sk
        and c.account_sk = d.account_sk
        and c.report_date = d.report_date
    where t.rows_now > 0 or c.load_sk is not null
)

select
    source::text as platform,
    kind::text as load_kind,
    age_days::integer as age_days,
    count(*) as replaced_days,
    count(*) filter (where changed_rows + vanished_rows > 0) as changed_days,
    round(
        (count(*) filter (where changed_rows + vanished_rows > 0))::numeric / count(*), 6
    ) as changed_days_share,
    sum(changed_rows)::bigint as changed_rows,
    sum(vanished_rows)::bigint as vanished_rows,
    round(sum(changed_rows + vanished_rows) / nullif(sum(rows_now), 0), 6) as changed_rows_share,
    round(avg(abs(spend_change) / nullif(spend_now, 0)), 6) as spend_shift,
    round(avg(abs(conversions_change) / nullif(conversions_now, 0)), 6) as conversions_shift,
    cast({{ reporting_currency() }} as char(3)) as reporting_currency,
    cast(coalesce(sum(spend_change_reporting), 0) as numeric(18,6)) as spend_change_reporting
from days
group by source, kind, age_days
