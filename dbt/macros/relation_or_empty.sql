{#- A table of a source, or, while no load has written it yet, an empty query with its columns,
    each of the type that columns gives it: so that the models of a source that a deployment may
    never load, such as the rates of the ECB (ADR-0032), build before its first load as after it.
    dbt replaces a view on every run, so the run after the first load reads the table. At parse
    time dbt runs nothing and takes the table, so the source still comes first in the graph. The
    empty query ends with its alias, so a macro that gives a table an alias of its own reads it
    through a subquery. -#}
{% macro relation_or_empty(relation, columns) -%}
    {%- if execute and load_relation(relation) is none -%}
        (select
            {%- for name, type in columns.items() %} cast(null as {{ type }}) as {{ name }}
            {%- if not loop.last %},{% endif %}
            {%- endfor %} where false) as {{ relation.identifier }}
    {%- else -%}
        {{ relation }}
    {%- endif -%}
{%- endmacro %}
