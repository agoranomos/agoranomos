-- The rate of each currency of the data into the reporting currency on each day (ADR-0025):
-- through the reference rates of the euro, each the last one published on or before the day. The
-- days run from the first published rate or the first day of the data, whichever comes first, to
-- the last day of either, and the data is that of every platform, as the unioned models hold it.
-- The reporting currency has the rate 1 on every day, whether or not a source publishes rates;
-- another currency has a rate from the first day that the source published one. The source is the
-- ECB once it has rates, and the demo only in a deployment without them, so that a day on which
-- the ECB publishes nothing, such as Good Friday, carries its last rate forward and never takes a
-- fictional one (ADR-0032).
with rates as (
    select rate_date, base_currency, quote_currency, rate, rate_source
    from {{ ref('stg_ecb__rates_daily') }}
    union all
    select rate_date, base_currency, quote_currency, rate, rate_source
    from {{ ref('stg_demo_fx__rates_daily') }}
    where not exists (select from {{ ref('stg_ecb__rates_daily') }})
),

published as (
    select rate_date, quote_currency as currency, rate as euro_rate, rate_source
    from rates
    where base_currency = 'EUR'
    union all
    select distinct rate_date, 'EUR', cast(1 as numeric(18,8)), rate_source
    from rates
),

facts as (
    select report_date, currency from {{ ref('int_ad_stats__unioned') }}
    union
    select report_date, currency from {{ ref('int_ad_conversions__unioned') }}
    union
    select report_date, currency from {{ ref('int_ad_stats_ad_group__unioned') }}
    union
    select report_date, currency from {{ ref('int_ad_stats_ad__unioned') }}
    union
    select report_date, currency from {{ ref('int_web_sessions__unioned') }}
),

days as (
    select cast(day as date) as rate_date
    from generate_series(
        cast(least(
            (select min(rate_date) from published), (select min(report_date) from facts)
        ) as timestamp),
        cast(greatest(
            (select max(rate_date) from published), (select max(report_date) from facts)
        ) as timestamp),
        interval '1 day'
    ) as day
),

currencies as (
    -- distinct: without it, PostgreSQL counts a currency for each day and currency of the facts,
    -- and plans for billions of rows of this view, which has about a thousand.
    select distinct currency from facts where currency is not null
    union
    select {{ reporting_currency() }}
),

latest as (
    select d.rate_date, c.currency, p.euro_rate, p.rate_date as published_on, p.rate_source
    from days as d
    cross join currencies as c
    left join lateral (
        select euro_rate, rate_date, rate_source
        from published
        where published.currency = c.currency and published.rate_date <= d.rate_date
        order by published.rate_date desc
        limit 1
    ) as p on true
)

select
    l.rate_date,
    l.currency,
    {{ reporting_currency() }} as reporting_currency,
    case
        when l.currency = {{ reporting_currency() }} then cast(1 as numeric(18,8))
        else cast(round(r.euro_rate / l.euro_rate, 8) as numeric(18,8))
    end as rate,
    case when l.currency = {{ reporting_currency() }} then 'identity' else l.rate_source end as rate_source,
    case
        when l.currency = {{ reporting_currency() }} then false
        else l.published_on < l.rate_date or r.published_on < r.rate_date
    end as is_carried_forward
from latest as l
join latest as r
    on r.rate_date = l.rate_date and r.currency = {{ reporting_currency() }}
where l.currency = {{ reporting_currency() }} or (l.euro_rate is not null and r.euro_rate is not null)
