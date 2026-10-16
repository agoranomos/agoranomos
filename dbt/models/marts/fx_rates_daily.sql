-- The rate of each currency of the data into the reporting currency on each day (ADR-0025).
select rate_date, currency, reporting_currency, rate, rate_source, is_carried_forward
from {{ ref('int_fx_rates__filled') }}
