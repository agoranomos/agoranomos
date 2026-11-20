{{ config(tags=['critical']) }}
-- ADR-0042, after the write of the rates: each rate of the file of the deployment,
-- extensions/model/rates.toml, of a currency of the accounts in a quote that is not the reporting
-- currency goes through the rate of its quote into the reporting currency, against the base of
-- the first source of the rates (core_fx_rates_daily), so the quote is a currency of which a
-- source of the order with that base, or the demo, gives a rate: in staging, in the rows of the
-- core, or as the second leg of a rate of the file that the core holds. A row for each quote of
-- which none gives one: an error, as the model could not convert the rates of the file in it.
{%- set chain = rates_chain() %}
{%- set base = rates_base(chain[0]) %}
{%- set staged = {
    'bank_of_italy': ref('stg_bank_of_italy__rates_daily'),
    'nbk': ref('stg_nbk__rates_daily'),
    'ecb': ref('stg_ecb__rates_daily'),
    'demo_fx': ref('stg_demo_fx__rates_daily'),
} %}
{%- set same_base = [] %}
{%- for code in chain + ['demo_fx'] if rates_base(code) == base %}
    {%- do same_base.append(code) %}
{%- endfor %}
with quoted as (
    select g.quote_currency::text as currency, count(*) as entries
    from {{ ref('stg_settings__rates') }} as g
    where g.quote_currency <> {{ reporting_currency() }}
        and g.currency <> {{ reporting_currency() }}
        and g.currency in (select currency from {{ ref('dim_account') }})
    group by g.quote_currency
),

rated as (
    select '{{ base }}' as currency
    {%- for code in same_base %}
    union
    select quote_currency::text
    from {{ staged[code] }}
    {%- endfor %}
    union
    select currency::text
    from {{ ref('core_fx_rates_daily') }}
    where rate_source in ('{{ same_base | join("', '") }}')
        and cross_source is null
    union
    select base_currency::text
    from {{ ref('core_fx_rates_daily') }}
    where rate_source = 'deployment'
        and cross_source in ('{{ same_base | join("', '") }}')
)

select q.currency as quote_currency, q.entries
from quoted as q
where not exists (select from rated as r where r.currency = q.currency)
