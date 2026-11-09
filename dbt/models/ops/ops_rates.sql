-- The age of the last rate that the source of the rates published into the reporting currency, as
-- the core holds it, with a warning after 3 business days (ADR-0042), which ./agoranomos status
-- reads too, as ops_loads gives the hours since each load. The business days are Monday to Friday
-- after the day of the last rate, up to the day on which the view is read, in the time zone of the
-- warehouse: the ECB publishes on the working days of TARGET, and its load of the morning takes
-- the days up to yesterday, so the rate of a day is 1 business day old on the next one, and more
-- than 3 is late. A closing day of TARGET counts as a business day.
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
)

select
    cast({{ reporting_currency() }} as char(3)) as reporting_currency,
    rate_source::text as rate_source,
    published_on as last_rate_date,
    business_days::integer as business_days_since_rate,
    business_days > 3 as is_late
from aged
