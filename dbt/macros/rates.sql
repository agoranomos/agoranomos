{#- The rates of the core (ADR-0042, ADR-0037): core.fx_rates_daily holds, for each calendar day,
    each currency of the accounts and the reporting currency, the rate of one unit of the currency
    in the reporting currency, through the base currency of the source of the rates with no
    rounding on the way, as Regulation (EC) 1103/97 asks, and carries the last published rate over
    a day without one, with a flag. The ECB stays the source until v0.6 (ADR-0032), and the demo
    serves only a deployment without it. The model core_fx_rates_daily writes the table, and the
    checks of G1 (quarantine.sql) find an account whose currency has no rate at all. -#}

{#- The source of the rates of this run, as SQL that gives its code: ecb once the ECB has a rate,
    in staging, ecb, the staging model of its rates, or in rates, core.fx_rates_daily as the core
    holds it, and else demo_fx. The core keeps the rates for good and the raw layer only the loads
    that the core has not accepted (ADR-0035), so a run that reads no load of the ECB still takes
    no rate of the demo once the core holds one of the ECB. -#}
{% macro rates_source(ecb, rates) -%}
(case
    when exists (select from {{ ecb }}) or exists (select from {{ rates }} where rate_source = 'ecb')
        then 'ecb'
    else 'demo_fx'
end)
{%- endmacro %}

{#- The rates that the source of this run published, of the loads that this run reads: one row for
    each day of publication and currency, the value of one unit of base_currency, the euro, in
    quote_currency, and the base currency itself at 1 on each day of publication, so that a rate
    into or from the euro goes through the same join as any other. ecb and demo are the staging
    models of the rates of the ECB and of the demo, rates is core.fx_rates_daily. A rate of zero or
    less, which no source publishes, is left out, as a rate through it has no value. -#}
{% macro rates_published(ecb, demo, rates) -%}
with read as (
    select rate_date, base_currency, quote_currency, rate, rate_source
    from {{ ecb }}
    union all
    select rate_date, base_currency, quote_currency, rate, rate_source
    from {{ demo }}
),

quoted as (
    select rate_date, base_currency, quote_currency, rate, rate_source
    from read
    where rate_source = {{ rates_source(ecb, rates) }}
        and rate > 0
        and quote_currency <> base_currency
)

select rate_date, base_currency, quote_currency, rate, rate_source
from quoted
union all
select distinct rate_date, base_currency, base_currency, cast(1 as numeric(18,8)), rate_source
from quoted
{%- endmacro %}

{#- The rates of the currencies into the reporting currency on their days of publication, from
    published, the rows of rates_published: a currency and the reporting currency published on the
    same day give the rate of one unit of the currency in the reporting currency, the rate of the
    reporting currency divided by that of the currency, with no rounding. The source publishes
    every currency of a day at once, as the ECB and the demo do, so a day of publication has both.
    The reporting currency itself is left out: its rate is 1, and it needs no source. -#}
{% macro rates_into_reporting(published) -%}
select
    c.rate_date as published_on,
    c.quote_currency as currency,
    c.base_currency,
    c.rate as base_rate,
    r.rate / c.rate as rate_into_reporting_currency,
    c.rate_source
from {{ published }} as c
join {{ published }} as r
    on r.rate_date = c.rate_date and r.quote_currency = {{ reporting_currency() }}
where c.quote_currency <> {{ reporting_currency() }}
{%- endmacro %}
