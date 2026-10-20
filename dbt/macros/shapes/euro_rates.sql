{#- The shape of a source of the reference rates of the euro: one rate of a currency for one euro
    on each day of publication. -#}
{% macro euro_rates(relation, platform) %}
select
    cast(rate_date as date) as rate_date,
    base as base_currency,
    quote as quote_currency,
    cast(rate as numeric(18,8)) as rate,
    '{{ platform }}' as rate_source
from {{ relation }}
{% endmacro %}
