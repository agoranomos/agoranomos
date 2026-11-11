-- The rate of each currency of the data into the reporting currency on each day, a view over the
-- core (ADR-0037, ADR-0042): the rows of core.fx_rates_daily of the reporting currency of the
-- deployment, as the core keeps those of a reporting currency of before. The column of the rate
-- has a name that does not end in rate: Metabase 0.64.1 gives such a decimal column the type of a
-- share, a percentage, each time that it analyzes it.
select
    r.rate_date,
    cast(r.currency as text) as currency,
    cast(r.reporting_currency as text) as reporting_currency,
    cast(r.rate_into_reporting_currency as numeric(18,8)) as rate_into_reporting_currency,
    r.rate_source,
    r.is_carried_forward
from {{ ref('core_fx_rates_daily') }} as r
where r.reporting_currency = {{ reporting_currency() }}
