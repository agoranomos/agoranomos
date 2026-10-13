{#- The numbers of the APIs as staging needs them (ADR-0025). The platforms send numbers as
    strings, and TikTok writes "-" for a number that means nothing on a day: each becomes NULL. -#}

{#- A whole number, such as impressions. -#}
{% macro to_count(expression) -%}
    cast(nullif(nullif(trim(cast({{ expression }} as text)), ''), '-') as bigint)
{%- endmacro %}

{#- Any other number, such as money in major units, to six decimal places. -#}
{% macro to_number(expression) -%}
    cast(nullif(nullif(trim(cast({{ expression }} as text)), ''), '-') as numeric(18,6))
{%- endmacro %}

{#- Money in millionths of a unit, as Google writes cost_micros, in units. -#}
{% macro micros_to_number(expression) -%}
    cast(cast(nullif(trim(cast({{ expression }} as text)), '') as numeric(20,0)) / 1000000 as numeric(18,6))
{%- endmacro %}
