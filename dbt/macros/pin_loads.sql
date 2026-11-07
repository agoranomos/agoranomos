{#- The pin of the loads (ADR-0046): the first task of dbt of each run of the transform runs
    dbt run-operation pin_loads, before its other tasks of dbt, and the run reads only the loads
    that it finds in core.loads, through stg_loads__newest_days, which pinned_loads fills from it.
    A load that ends later, between two tasks of dbt of the run, waits for the next run: a pin in
    on-run-start would pin again in every call of dbt, and each node of a run is one (ADR-0028).
    So only a run of the DAG pins, which sets DBT_JOB_RUN_ID: dbt by hand reads what is pinned and
    pins nothing.

    In one transaction, the macro copies into core.loads, for each source whose journal and
    _dlt_loads exist, the rows of the journal of every whole load, which _dlt_loads has with status
    0, that core.loads does not hold yet: the window or the error of each account of the load, and
    the moment when the load ended. A source that no load has written is passed over, as a
    deployment without the demo never loads the sources of the demo. The ID of the call of dbt
    records which pin took a row. Then it creates, through core.ensure_partition, the partitions of
    the facts that each source is to write, for every month of the windows that are pinned, each in
    a transaction of its own and outside any model: a hook before_begin of a model breaks the
    transaction of the model (#191). The rows stay pinned until the core accepts their loads.
    transformer runs it, with its rights on core.loads and on core.ensure_partition. -#}
{% macro pin_loads() %}
    {%- if env_var('DBT_JOB_RUN_ID', '') == '' -%}
        {{- exceptions.raise_compiler_error(
            'pin_loads pins only in a run of the DAG agoranomos_demo_transform, which sets '
            ~ 'DBT_JOB_RUN_ID: dbt by hand reads the loads that a run pinned'
        ) -}}
    {%- endif -%}
    {%- set loads = pinned_loads_journal() -%}
    {%- set pinned = [] -%}
    {%- for source_name in newest_load_sources() -%}
        {%- set source = source_name[4:] -%}
        {%- set journal = adapter.get_relation(
            database=target.database, schema=source_name, identifier='_agoranomos_loads'
        ) -%}
        {%- set finished = adapter.get_relation(
            database=target.database, schema=source_name, identifier='_dlt_loads'
        ) -%}
        {%- if journal is not none and finished is not none -%}
            {%- call statement('pin', fetch_result=false) -%}
insert into {{ loads }} (
    source, load_id, account_id, window_from, window_to, outcome, error, finished_at, invocation_id
)
select
    '{{ source }}', j._dlt_load_id, j.account_id, j.window_from, j.window_to, j.status, j.error,
    l.inserted_at, '{{ invocation_id }}'::uuid
from {{ journal }} as j
join {{ finished }} as l
    on l.load_id = j._dlt_load_id and l.status = 0
where not exists (
    select from {{ loads }} as p
    where p.source = '{{ source }}' and p.load_id = j._dlt_load_id and p.account_id = j.account_id
)
on conflict (source, load_id, account_id) do nothing
            {%- endcall -%}
            {%- do pinned.append(source ~ ' ' ~ load_result('pin')['response'].rows_affected) -%}
        {%- endif -%}
    {%- endfor -%}
    {#- The months of the windows that are pinned, for each fact that their source is to write,
        read in the transaction of the pin, which then commits. -#}
    {%- set facts = [] -%}
    {%- for source_name, tables in pinned_load_facts().items() -%}
        {%- for table in tables -%}
            {%- do facts.append("('" ~ source_name[4:] ~ "', '" ~ table ~ "')") -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- call statement('months', fetch_result=true) -%}
select f.fact, to_char(m.month, 'YYYY-MM-DD')
from {{ loads }} as l
join (values {{ facts | join(', ') }}) as f (source, fact)
    on f.source = l.source
cross join lateral generate_series(
    date_trunc('month', l.window_from::timestamp), l.window_to::timestamp, interval '1 month'
) as m (month)
where l.status = 'pinned' and l.outcome = 'loaded'
group by 1, 2
order by 1, 2
    {%- endcall -%}
    {%- set months = load_result('months')['data'] -%}
    {%- do adapter.commit() -%}
    {%- set created = [] -%}
    {%- for fact, month in months -%}
        {%- call statement('partition', fetch_result=true) -%}
select core.ensure_partition('core.{{ fact }}'::regclass, date '{{ month }}')
        {%- endcall -%}
        {%- do adapter.commit() -%}
        {%- if load_result('partition')['data'][0][0] -%}
            {%- do created.append(fact) -%}
        {%- endif -%}
    {%- endfor -%}
    {{- log(
        'Pinned the new rows of the journals of the loads: ' ~ ((pinned | join(', ')) or 'none')
        ~ '. The pinned windows take ' ~ (months | length) ~ ' months of the facts, of which '
        ~ (created | length) ~ ' got their partition now.',
        info=True
    ) -}}
{% endmacro %}

{#- The partitioned facts of the core that each source is to write (ADR-0036), whose partitions
    the pin creates for the months of its windows: the four facts of the ad platforms, and that of
    web analytics. The rates go into core.fx_rates_daily, which has no partitions. Its sources are
    those of newest_load_sources, and tests/unit/test_dbt_project.py checks both lists against the
    migrations. -#}
{% macro pinned_load_facts() -%}
    {%- set ads = [
        'ad_stats_campaign_daily', 'ad_stats_ad_group_daily', 'ad_stats_ad_daily',
        'ad_conversions_campaign_daily',
    ] -%}
{{- return({
    'raw_demo_fx': [],
    'raw_demo_search': ads,
    'raw_demo_social': ads,
    'raw_demo_video': ads,
    'raw_demo_web': ['web_sessions_daily'],
    'raw_ecb': [],
    'raw_google_ads': ads,
}) -}}
{%- endmacro %}
