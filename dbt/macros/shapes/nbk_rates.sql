{#- The shape of the feed of the official rates of the National Bank of Kazakhstan, rss/get_rates.cfm,
    as dlt loads it, every column text, as the NBK writes it: for each currency (title) on each
    calendar day (date, DD.MM.YYYY), the price in tenge (description) of quant units of it. The
    macro turns them into the columns of the rates of a source, for the source nbk (ADR-0042),
    against KZT as the base, in two directions:
    * rate, as rates_published reads the rates of a source: the units of quote_currency for one
      unit of base_currency, one tenge, so quant divided by description, with no rounding but that
      of the division of numeric, at least 16 significant digits, where the eight decimal places of
      the ECB would leave six of 1 / 444.76;
    * base_rate, in the convention of the NBK: the tenge for one unit of quote_currency, so
      description divided by quant, exact, as quant is a power of ten.
    An item whose price or quant is not a positive decimal number is left out. It takes the
    relation of the raw table and the code of the source. -#}

{% macro nbk_rates(relation, platform) %}
select
    rate_date,
    cast('KZT' as text) as base_currency,
    quote_currency,
    units / price as rate,
    price / units as base_rate,
    '{{ platform }}' as rate_source
from (
    select
        to_date("date", 'DD.MM.YYYY') as rate_date,
        title as quote_currency,
        case when description ~ '^[0-9]+(\.[0-9]+)?$' then cast(description as numeric) end as price,
        case when quant ~ '^[0-9]+$' then cast(quant as numeric) end as units
    from {{ relation }}
) as items
where price > 0 and units > 0
{% endmacro %}
