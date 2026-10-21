-- Each test of the project in each run of dbt that the hooks of elementary recorded: its status and
-- the number of rows that it found, without the rows themselves (test_sample_row_count is 0 in
-- dbt_project.yml) and without its SQL.
select
    invocations.job_id as airflow_dag_id,
    invocations.job_run_id as airflow_run_id,
    results.invocation_id,
    -- elementary writes the moment in UTC, without a time zone.
    results.detected_at at time zone 'UTC' as detected_at,
    results.test_unique_id as test_id,
    results.test_short_name as test_type,
    results.schema_name,
    results.table_name as model_name,
    results.column_name,
    -- dbt writes the severity of a test as its config gives it: error by default, in capitals.
    lower(results.severity) as severity,
    results.status,
    results.failures
from {{ ref('elementary', 'elementary_test_results') }} as results
left join {{ ref('elementary', 'dbt_invocations') }} as invocations
    on invocations.invocation_id = results.invocation_id
