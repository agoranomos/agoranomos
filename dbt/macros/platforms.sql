{#- The platforms of the model, as rows written into the text of a view of marts (ADR-0037): the
    rows of seed, the seed of the platforms of the base package, which the model passes as its ref,
    so that dbt loads the seed first. A view of marts reads the core alone: a run of the transform
    loads every seed again, by a TRUNCATE that waits for each query that reads it and that each
    later query waits for, which a view over the seed would bring to the dashboards. The platforms
    are code of the base package, so the rows of a view change with its definition, when the view
    is built again (marts_view.sql). dbt runs nothing when it parses the project, and the rows
    are then none. A column of the seed that the table of the seed does not hold yet, as in a
    warehouse of an earlier release before the transform has loaded the seed of this one, reads as
    none: marts-init builds the views before any run of the transform. -#}
{% macro platforms(seed) -%}
    {%- set columns = ['platform', 'platform_name', 'kind', 'mimics', 'is_demo', 'restatement_window_days'] -%}
    {%- set rows = [] -%}
    {%- if execute -%}
        {%- set held = adapter.get_columns_in_relation(seed) | map(attribute='name') | list -%}
        {%- set read = [] -%}
        {%- for column in columns -%}
            {%- do read.append(column if column in held else 'null as ' ~ column) -%}
        {%- endfor -%}
        {%- set found = run_query(
            'select ' ~ read | join(', ') ~ ' from ' ~ seed ~ ' order by platform'
        ) -%}
        {%- set rows = found.rows -%}
    {%- endif -%}
    {%- if rows | length == 0 -%}
(select
    cast(null as text) as platform,
    cast(null as text) as platform_name,
    cast(null as text) as kind,
    cast(null as text) as mimics,
    cast(null as boolean) as is_demo,
    cast(null as bigint) as restatement_window_days
where false)
    {%- else -%}
(values
    {%- for row in rows %}
    (
        {%- for value in row[:4] -%}
        {{ 'cast(null as text)' if value is none else "cast('" ~ (value | replace("'", "''")) ~ "' as text)" }},{{ ' ' }}
        {%- endfor -%}
        {{ 'cast(null as boolean)' if row[4] is none else ('true' if row[4] else 'false') }},{{ ' ' }}
        {{- 'cast(null as bigint)' if row[5] is none else 'cast(' ~ (row[5] | int) ~ ' as bigint)' }}
    ){% if not loop.last %},{% endif %}
    {%- endfor %}
)
    {%- endif %} as p ({{ columns | join(', ') }})
{%- endmacro %}


{#- The pre-hook of the seed of the platforms, in the transaction of the seed: the columns that a
    release added to the seed, which the table of the seed of an earlier release lacks. dbt loads a
    seed whose table exists by a TRUNCATE and an INSERT of the columns of its file, which would fail
    on such a column, so the hook adds it first: restatement_window_days, of v0.6. A new warehouse
    has no table yet, and dbt creates it from the file. -#}
{% macro add_platform_columns(seed) -%}
alter table if exists {{ seed }} add column if not exists restatement_window_days integer
{%- endmacro %}
