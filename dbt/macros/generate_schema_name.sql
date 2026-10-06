{#- A model goes to the schema that dbt_project.yml gives it, staging or marts, as it is: by default
    dbt would prefix it with the schema of the target, as in marts_staging. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
