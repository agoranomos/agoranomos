{{ config(tags=['critical']) }}
-- G3 of ADR-0035 and ADR-0042, after the write of the rates: each day of the windows that this run
-- writes into the facts of the core, int_window_days, has a rate of the currency of its account
-- into the reporting currency in core.fx_rates_daily, so that a view converts each row of a fact
-- with one join. A row for each currency that lacks a rate on some of those days: an error, as an
-- amount of such a day would have no amount in the reporting currency, which leaves the run red.
-- A currency that the source of the rates lacks keeps its accounts out of the core before the
-- write (macros/quarantine.sql), so a row here is a day before the first rate that the source
-- published of the currency, or a fault of the model.
select
    w.currency,
    count(distinct w.report_date) as days,
    min(w.report_date) as first_day,
    max(w.report_date) as last_day
from {{ ref('int_window_days') }} as w
where not exists (
    select
    from {{ ref('core_fx_rates_daily') }} as r
    where r.rate_date = w.report_date
        and r.currency = w.currency
        and r.reporting_currency = {{ reporting_currency() }}
)
group by w.currency
