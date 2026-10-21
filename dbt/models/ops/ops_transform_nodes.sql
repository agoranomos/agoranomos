-- Each node of the project in each run of dbt that the hooks of elementary recorded, the models,
-- seeds and tests of the project, without those of elementary itself. A task of the transform runs
-- dbt for one node, so a run of the transform is many runs of dbt, which share the ID of the run of
-- Airflow that the task gives dbt (agoranomos_demo_transform.py).
select
    invocations.job_id as airflow_dag_id,
    invocations.job_run_id as airflow_run_id,
    results.invocation_id,
    results.unique_id as node_id,
    results.name as node_name,
    results.resource_type,
    results.status,
    results.execution_time as execution_seconds,
    -- elementary writes the moments as ISO text in UTC, and none for a node that was skipped.
    nullif(results.execute_started_at, '')::timestamptz as started_at,
    nullif(results.execute_completed_at, '')::timestamptz as completed_at,
    results.rows_affected,
    results.failures,
    results.message
from {{ ref('elementary', 'dbt_run_results') }} as results
left join {{ ref('elementary', 'dbt_invocations') }} as invocations
    on invocations.invocation_id = results.invocation_id
where results.unique_id not like '%.elementary.%'
