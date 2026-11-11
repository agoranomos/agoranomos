-- The steps of the DAG of maintenance, agoranomos_maintenance, on the tables of the warehouse
-- (ADR-0039): its tasks write one row for each step on one table, and delete the rows older than
-- 90 days (airflow/dags/agoranomos_pipelines/maintenance.py). dbt only makes the table, empty,
-- with its columns, as elementary makes its own: a run adds no row, a column that a later release
-- declares is added, and full_refresh false keeps the rows from dbt run --full-refresh.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    full_refresh=false,
    on_schema_change='append_new_columns',
) }}

select
    cast(null as timestamp with time zone) as finished_at,
    cast(null as text) as maintenance_run_id,
    cast(null as text) as task,
    cast(null as text) as target,
    cast(null as numeric(12, 3)) as seconds,
    cast(null as bigint) as rows_deleted
where false
