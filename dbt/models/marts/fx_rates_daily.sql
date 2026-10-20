-- The rate of each currency of the data into the reporting currency on each day (ADR-0025). The
-- column of the rate has a name that does not end in rate: Metabase 0.64.1 gives such a decimal
-- column the type of a share, a percentage, each time that it analyzes it.
select
    rate_date,
    currency,
    reporting_currency,
    rate as rate_into_reporting_currency,
    rate_source,
    is_carried_forward
from {{ ref('int_fx_rates__filled') }}
