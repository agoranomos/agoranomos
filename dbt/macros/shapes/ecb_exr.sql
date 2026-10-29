{#- The shape of the data API of the European Central Bank: the CSV of its series of exchange rates
    (EXR) as dlt loads it, every column text, as the ECB writes it. The macro turns the daily
    reference rates of the euro into the columns that euro_rates gives, for the source ecb
    (ADR-0032). It takes the relation of the raw table and the code of the source. -#}

{% macro ecb_exr(relation, platform) %}
select
    cast(time_period as date) as rate_date,
    currency_denom as base_currency,
    currency as quote_currency,
    cast(obs_value as numeric(18,8)) as rate,
    '{{ platform }}' as rate_source
from {{ relation }}
where freq = 'D' and exr_type = 'SP00' and exr_suffix = 'A'
{% endmacro %}
