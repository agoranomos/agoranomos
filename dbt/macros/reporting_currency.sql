{#- The reporting currency of the deployment, as an SQL literal: the setting
    AGORANOMOS_REPORTING_CURRENCY of .env, which compose.yaml passes to dbt (ADR-0025). The YAML
    writes the same expression, as dbt renders env_var() there and a var holding it not. -#}
{% macro reporting_currency() -%}
    '{{ env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') }}'
{%- endmacro %}
