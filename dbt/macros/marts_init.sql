{#- The lock between the transform and the one-shot job marts-init (ADR-0039, ADR-0044), which
    builds the views of marts on every ./agoranomos up, outside Airflow, where the max_active_runs
    of the transform does not reach it. marts-init holds the advisory lock of PostgreSQL of the key
    marts_init_lock() for the whole of its build, exclusive, from a connection of its own
    (metabase/marts_init.py). Each node of a run of the transform that writes, a model, a seed or a
    snapshot, takes the same lock, shared, in the transaction in which it writes, through the
    pre-hook wait_for_marts_init of dbt_project.yml, and the pin holds it, shared, through all of
    its transactions (pin_loads.sql). So a node waits while marts-init builds, and marts-init waits
    for the nodes that write when it starts; a request of marts-init that waits goes before the
    nodes that come after it. The nodes of the transform share the lock with each other. Only a
    run of the DAG takes it, which sets DBT_JOB_RUN_ID: the nodes of marts-init itself, and dbt by
    hand, do not. A node waits longer than the lock timeout of transformer, 1 minute: up to 10
    minutes, the execution timeout of a task of the transform, and then fails, and the next run
    repeats it. -#}

{#- The key of the lock, a number of PostgreSQL's bigint, which metabase/marts_init.py holds too:
    the letters agor in ASCII. -#}
{% macro marts_init_lock() -%}
    {{- return(1634168690) -}}
{%- endmacro %}

{#- The pre-hook of every model, seed and snapshot of the project: the lock, shared, until the
    transaction of the node ends, in a run of the DAG of the transform only. The lock timeout is
    the one of the role again once the lock is held. -#}
{% macro wait_for_marts_init() -%}
    {%- if env_var('DBT_JOB_RUN_ID', '') != '' -%}
set local lock_timeout = '10min';
select pg_advisory_xact_lock_shared({{ marts_init_lock() }});
set local lock_timeout to default
    {%- endif -%}
{%- endmacro %}
