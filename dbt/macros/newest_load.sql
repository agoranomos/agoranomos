{#- The rows of a fact table of a source in raw that its staging model reads (ADR-0038, ADR-0046).
    Each load of a source appends its rows under its own _dlt_load_id, and a row for each account
    to the journal _agoranomos_loads of its dataset in the same run, so a table holds the rows of
    every load that the core has not accepted yet (ADR-0035). A run of the transform reads only the
    loads that it pinned, in one task before its tasks of dbt (pin_loads.sql): they are in
    core.loads, and a load that ends later waits for the next run, however many calls of dbt the
    run makes. For each account and day, staging reads the rows of the newest pinned load whose
    window covers the day (pinned_loads):
    * a load is pinned once it is whole, when _dlt_loads has its row with status 0, which dlt
      writes after the last table of the load, so no row of a load that stopped half way is read;
    * an account counts in a load when its row of the journal has the status loaded: one that
      failed has no window, and its days keep the load before;
    * the newest load of a day is the one with the largest load id, a time in seconds as text with
      a fraction of varying length, which compares as a number;
    * a day that the newest load covering it does not hold is empty, so a load replaces its days
      whole, and a row that a platform no longer sends goes; a day of a shorter window keeps the
      older load that covers it (ADR-0046).
    The newest pinned load of each account and day is the table stg_loads__newest_days, which each
    run of the transform builds once, before the staging views of the facts, with the statistics
    of PostgreSQL: a view in their place made each of them a subquery that the planner took for one
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
    from ({{ pinned_loads(source_name, pinned_loads_journal()) }}) as days
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

{#- For each account and day of the windows of the pinned loads of a source, the newest load that
    covers it, from loads, the relation of core.loads, with the key of newest_load_key: the rows of
    stg_loads__newest_days of one source (ADR-0046).
    * The newest load is chosen among the pinned and the accepted loads of the account, and a day
      is read only when it is a pinned one, as the core holds the days of an accepted load: so an
      older load that ends after a newer one was accepted does not replace its days.
    * Of the accepted loads, only those newer than the oldest pinned load of the account count,
      as only such a load can win a day of a pinned one: the accepted loads of 13 months would
      otherwise unroll about 180 windows of 90 days for each account (#191).
    * A failed account has no window, and its row is passed over.
    A run that succeeds accepts the loads that it read (agoranomos_pipelines/acceptance.py), so
    the next run reads only the loads pinned since, and those of an account in quarantine. -#}
{% macro pinned_loads(source_name, loads) -%}
    {%- set account, _ = newest_load_keys(source_name) -%}
    {%- set source = source_name[4:] -%}
select
    '{{ source }}' as source,
    d.account_id,
    d.day,
    d.load_id,
    {{ newest_load_key(source_name, 'd.load_id', none if account is none else 'd.account_id', 'd.day') }} as load_key
from (
    select distinct on (l.account_id, w.day)
        l.account_id,
        w.day,
        l.load_id,
        l.status
    from (
        select p.account_id, p.load_id, p.window_from, p.window_to, p.status
        from {{ loads }} as p
        where p.source = '{{ source }}' and p.status = 'pinned' and p.outcome = 'loaded'
        union all
        select a.account_id, a.load_id, a.window_from, a.window_to, a.status
        from {{ loads }} as a
        join (
            select account_id, min(load_id::numeric) as load_number
            from {{ loads }}
            where source = '{{ source }}' and status = 'pinned' and outcome = 'loaded'
            group by account_id
        ) as oldest
            on oldest.account_id = a.account_id and a.load_id::numeric > oldest.load_number
        where a.source = '{{ source }}' and a.status = 'accepted' and a.outcome = 'loaded'
    ) as l
    cross join lateral (
        select l.window_from + n as day
        from generate_series(0, l.window_to - l.window_from) as n
    ) as w
    order by l.account_id, w.day, l.load_id::numeric desc
) as d
where d.status = 'pinned'
{%- endmacro %}

{#- core.loads, the journal of the loads of the core, which a migration creates (ADR-0038): the
    relation for the macros, as a model that reads it names source('core', 'loads') in its text. -#}
{% macro pinned_loads_journal() -%}
    {{- return(api.Relation.create(database=target.database, schema='core', identifier='loads')) -}}
{%- endmacro %}

{#- The journal of a source, or, while no load has written it, an empty query with its columns
    (relation_or_empty): a deployment without the demo never loads the sources of the demo, and a
    deployment may never load the rates of the ECB or Google Ads. -#}
{% macro newest_load_table(source_name, table_name) -%}
    {{- relation_or_empty(source(source_name, table_name), newest_load_columns()[table_name]) -}}
{%- endmacro %}

{#- The columns of the journal that ops_loads reads, each of the type that dlt gives it in
    PostgreSQL. -#}
{% macro newest_load_columns() -%}
{{- return({
    '_agoranomos_loads': {
        'account_id': 'character varying', 'as_of': 'date', 'window_from': 'date',
        'window_to': 'date', 'now': 'timestamp with time zone',
        'loaded_at': 'timestamp with time zone', 'status': 'character varying',
        'error': 'character varying', '_dlt_load_id': 'character varying',
        '_dlt_id': 'character varying',
    },
}) -}}
{%- endmacro %}

{#- The sources whose fact tables staging reads by load, and whose loads the task of the pin
    pins (pin_loads), with how their rows, r, name the account and the day, as SQL: the account as
    the journal names it, and the day as a date. The rates have one account, the base of the
    rates, and their rows none (none): the day alone finds the load. The demo platforms take these
    from airflow/dags/agoranomos_pipelines/demo/platforms.py, and tests/unit/test_dbt_project.py
    checks that the two agree; Google Ads has the shape of demo_search. -#}
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

{#- The rows of a table of the entities of a source in raw that its staging model reads: the list
    of each account in the newest load of the account that a run pinned (ADR-0038). Each load of a
    source appends the full list of each account that it took, of its accounts, campaigns, ad
    groups, ads or conversion actions, under its own _dlt_load_id, in the same run as its facts
    and its journal, so a list is whole once its load is, and staging reads, for each account, the
    rows of the newest load that took it (newest_lists): an account that failed in a load keeps the
    list of the load before, and an entity that the newest list no longer names is not read, so
    that the dimension of the core can say that the platform no longer lists it. relation is the
    table, through demo_source or relation_or_empty, with its column _dlt_load_id, source_name its
    source, loads the relation of core.loads, which the model passes as source('core', 'loads'),
    so that its dependency is in its text, and account how its rows, r, name the account as the
    journal does, as SQL: by default as newest_load_keys gives it for the facts of the source. The
    result is a subquery with an alias, as relation_or_empty gives one. -#}
{% macro newest_list(relation, source_name, loads, account=none) -%}
    {%- set account = newest_load_keys(source_name)[0] if account is none else account -%}
(select r.*
from (select * from {{ relation }}) as r
join ({{ newest_lists(source_name, loads) }}) as l
    on l.load_id = r._dlt_load_id and l.account_id = {{ account }}
) as newest_list
{%- endmacro %}

{#- For each account of a source, the newest load whose lists staging reads, from loads, the
    relation of core.loads, with the moment when it ended: the source, the account, the load and
    finished_at.
    * The newest load is the one with the largest load id among the pinned and the accepted loads
      that took the account, and its lists are read only when it is a pinned one, as the core
      holds those of an accepted load: so an older load that ends after a newer one was accepted
      does not bring back an older list.
    * Of the accepted loads, only those newer than the oldest pinned load of the account count, as
      only such a load can be newer than every pinned one (pinned_loads).
    * A failed account has no list in its load, and its row is passed over.
    An account whose newest load is accepted has no list here, and its dimensions keep what the
    core holds. -#}
{% macro newest_lists(source_name, loads) -%}
    {%- set source = source_name[4:] -%}
select n.source, n.account_id, n.load_id, n.finished_at
from (
    select distinct on (l.account_id) l.source, l.account_id, l.load_id, l.finished_at, l.status
    from {{ loads }} as l
    join (
        select account_id, min(load_id::numeric) as load_number
        from {{ loads }}
        where source = '{{ source }}' and status = 'pinned' and outcome = 'loaded'
        group by account_id
    ) as oldest
        on oldest.account_id = l.account_id and l.load_id::numeric >= oldest.load_number
    where l.source = '{{ source }}' and l.outcome = 'loaded'
    order by l.account_id, l.load_id::numeric desc
) as n
where n.status = 'pinned'
{%- endmacro %}
