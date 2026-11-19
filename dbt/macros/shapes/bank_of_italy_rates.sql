{#- The shape of the REST API of the exchange rates of the Bank of Italy: the items of the answers
    of dailyRates against the euro as dlt loads them, every column text, as the bank writes them.
    The macro turns them into the columns that euro_rates gives, for the source bank_of_italy
    (ADR-0042). It takes the relation of the raw table and the code of the source.
    * The bank lists a currency once for each country that uses it, such as XAF, XCD and XOF, with
      the same rate on a day: one row is kept for each day and ISO code, of the first country by
      name.
    * avg_rate is N.A. for a currency that has no rate on the day, such as RUB as of November
      2026: only a rate written as a number is kept and cast, so N.A. never is.
    * The convention of a rate is read from its text, the amount of the currency for one euro, and
      not from exchange_convention_code: the live answers give C where the Operating Instructions
      show I for the same text. -#}

{% macro bank_of_italy_rates(relation, platform) %}
select
    rate_date,
    base_currency,
    quote_currency,
    cast(avg_rate as numeric(18,8)) as rate,
    '{{ platform }}' as rate_source
from (
    select distinct on (cast(reference_date as date), iso_code)
        cast(reference_date as date) as rate_date,
        'EUR' as base_currency,
        iso_code as quote_currency,
        avg_rate
    from {{ relation }}
    where avg_rate ~ '^[0-9]+(\.[0-9]+)?$'
        and lower(trim(exchange_convention)) like 'foreign currency amount for 1 euro%'
    order by cast(reference_date as date), iso_code, country
) as rates
{% endmacro %}
