{#- The rows of a fact table of a source in raw that its staging model reads (ADR-0038). Each load
    of a source appends its rows under its own _dlt_load_id, and a row for each account to the
    journal _agoranomos_loads of its dataset in the same run, so a table holds the rows of every
    load that the core has not accepted yet (ADR-0035). For each account and day, staging reads the
    rows of the newest whole load whose window covers the day:
    * a load is whole once _dlt_loads has its row with status 0, which dlt writes after the last
      table of the load, so no row of a load that stopped half way is read;
    * an account counts in a load when its row of the journal has the status loaded: one that
      failed has no window, and its days keep the load before;
    * the newest load of a day is the one with the largest load id, a time in seconds as text with
      a fraction of varying length, which compares as a number;
    * a day that the newest load covering it does not hold is empty, so a load replaces its days
      whole, and a row that a platform no longer sends goes; a day of a shorter window keeps the
      older load that covers it (ADR-0046).
    The newest load of each account and day is the table stg_loads__newest_days, which each run of
    the transform builds once, before the staging views of the facts, with the statistics of
    PostgreSQL: a view in their place made each of them a subquery that the planner took for one
    row, and that the views above them repeated. relation is the table, through demo_source or
    relation_or_empty, with its column _dlt_load_id, source_name its source, the schema
    raw_<source>, whose rows name the account and the day as newest_load_keys says, and days the
    ref of stg_loads__newest_days, which the model passes, so that its dependency is in its text
    (tests/unit/test_dbt_project.py). The result is a subquery with an alias, as relation_or_empty
    gives one. -#}
{% macro newest_load(relation, source_name, days) -%}
    {%- set account, day = newest_load_keys(source_name) -%}
(select r.*
from (select * from {{ relation }}) as r
join (
    select load_key
    {#- The table, or, while a run has not filled it yet, its rows of the source as a query: the
        first run of the transform after an update that brings it can come from the DAG of the
        release before, which the DAG processor renders again within five minutes. #}
    {%- if execute and load_relation(days) is none %}
    from ({{ newest_load_days(source_name) }}) as days
    {%- else %}
    from {{ days }}
    {%- endif %}
    where source = '{{ source_name[4:] }}'
) as d
    on d.load_key = {{ newest_load_key(source_name, 'r._dlt_load_id', account, day) }}
) as newest_load
{%- endmacro %}

{#- The key of a load, an account and a day of a source, as SQL from the SQL of each: one column,
    so that the planner estimates the join of a table of raw with its days from the statistics of
    that column, where three columns would multiply three guesses. The day goes in as its number
    of days since 2000, which reads the same in every session. -#}
{% macro newest_load_key(source_name, load_id, account, day) -%}
    '{{ source_name[4:] }}|' || {{ load_id }}
    {%- if account is not none %} || '|' || {{ account }}{% endif %}
    || '|' || ({{ day }} - date '2000-01-01')::text
{%- endmacro %}

{#- For each account and day of the windows of the whole loads of a source, its newest load, from
    the journal of the source and the _dlt_loads of its dataset, with the key of newest_load_key:
    the rows of stg_loads__newest_days of one source. -#}
{% macro newest_load_days(source_name) -%}
    {%- set account, _ = newest_load_keys(source_name) -%}
select
    '{{ source_name[4:] }}' as source,
    d.account_id,
    d.day,
    d.load_id,
    {{ newest_load_key(source_name, 'd.load_id', none if account is none else 'd.account_id', 'd.day') }} as load_key
from (
    select distinct on (j.account_id, w.day)
        j.account_id,
        w.day,
        j._dlt_load_id as load_id
    from (select * from {{ newest_load_table(source_name, '_agoranomos_loads') }}) as j
    join (select * from {{ newest_load_table(source_name, '_dlt_loads') }}) as l
        on l.load_id = j._dlt_load_id and l.status = 0
    cross join lateral (
        select j.window_from + n as day
        from generate_series(0, j.window_to - j.window_from) as n
    ) as w
    where j.status = 'loaded'
    order by j.account_id, w.day, j._dlt_load_id::numeric desc
) as d
{%- endmacro %}

{#- The journal or _dlt_loads of a source, or, while no load has written it, an empty query with
    their columns (relation_or_empty): a deployment without the demo never loads the sources of
    the demo, and a deployment may never load the rates of the ECB or Google Ads. -#}
{% macro newest_load_table(source_name, table_name) -%}
    {{- relation_or_empty(source(source_name, table_name), newest_load_columns()[table_name]) -}}
{%- endmacro %}

{#- The columns of the journal and of _dlt_loads that newest_load and ops_loads read, each of the
    type that dlt gives it in PostgreSQL. -#}
{% macro newest_load_columns() -%}
{{- return({
    '_agoranomos_loads': {
        'account_id': 'character varying', 'as_of': 'date', 'window_from': 'date',
        'window_to': 'date', 'now': 'timestamp with time zone',
        'loaded_at': 'timestamp with time zone', 'status': 'character varying',
        'error': 'character varying', '_dlt_load_id': 'character varying',
        '_dlt_id': 'character varying',
    },
    '_dlt_loads': {'load_id': 'character varying', 'status': 'bigint'},
}) -}}
{%- endmacro %}

{#- The sources whose fact tables staging reads by load, and how their rows, r, name the account
    and the day, as SQL: the account as the journal names it, and the day as a date. The rates
    have one account, the base of the rates, and their rows none (none): the day alone finds the
    load. The demo platforms take these from airflow/dags/agoranomos_pipelines/demo/platforms.py,
    and tests/unit/test_dbt_project.py checks that the two agree; Google Ads has the shape of
    demo_search. -#}
{% macro newest_load_sources() -%}
{{- return({
    'raw_demo_fx': (none, 'r.rate_date'),
    'raw_demo_search': ('r.customer__id', 'r.segments__date::date'),
    'raw_demo_social': ('r.account_id', 'r.date_start'),
    'raw_demo_video': ('r.advertiser_id', 'left(r.dimensions__stat_time_day::text, 10)::date'),
    'raw_demo_web': ("split_part(r.property, '/', 2)", "to_date(r.date, 'YYYYMMDD')"),
    'raw_ecb': (none, 'cast(r.time_period as date)'),
    'raw_google_ads': ('r.customer__id', 'r.segments__date::date'),
}) -}}
{%- endmacro %}

{#- How the fact tables of one source name the account and the day (newest_load_sources). -#}
{% macro newest_load_keys(source_name) -%}
{%- set sources = newest_load_sources() -%}
{%- if source_name not in sources -%}
    {{- exceptions.raise_compiler_error('newest_load knows no source ' ~ source_name) -}}
{%- endif -%}
{{- return(sources[source_name]) -}}
{%- endmacro %}
