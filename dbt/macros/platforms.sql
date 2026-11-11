{#- The platforms of the model, as rows written into the text of a view of marts (ADR-0037): the
    rows of seed, the seed of the platforms of the base package, which the model passes as its ref,
    so that dbt loads the seed first. A view of marts reads the core alone: a run of the transform
    loads every seed again, by a TRUNCATE that waits for each query that reads it and that each
    later query waits for, which a view over the seed would bring to the dashboards. The platforms
    are code of the base package, so the rows of a view change with its definition, when the view
    is built again (marts_view.sql). dbt runs nothing when it parses the project, and the rows
    are then none. -#}
{% macro platforms(seed) -%}
    {%- set columns = ['platform', 'platform_name', 'kind', 'mimics', 'is_demo'] -%}
    {%- set rows = [] -%}
    {%- if execute -%}
        {%- set found = run_query(
            'select ' ~ columns | join(', ') ~ ' from ' ~ seed ~ ' order by platform'
        ) -%}
        {%- set rows = found.rows -%}
    {%- endif -%}
    {%- if rows | length == 0 -%}
(select
    cast(null as text) as platform,
    cast(null as text) as platform_name,
    cast(null as text) as kind,
    cast(null as text) as mimics,
    cast(null as boolean) as is_demo
where false)
    {%- else -%}
(values
    {%- for row in rows %}
    (
        {%- for value in row[:4] -%}
        {{ 'cast(null as text)' if value is none else "cast('" ~ (value | replace("'", "''")) ~ "' as text)" }},{{ ' ' }}
        {%- endfor -%}
        {{ 'cast(null as boolean)' if row[4] is none else ('true' if row[4] else 'false') }}
    ){% if not loop.last %},{% endif %}
    {%- endfor %}
)
    {%- endif %} as p ({{ columns | join(', ') }})
{%- endmacro %}
