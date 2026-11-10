-- The disk and the size of the warehouse, as the DAG of maintenance, agoranomos_maintenance,
-- records them once a day (ADR-0039): its task record_the_disk writes one row, and deletes the
-- rows older than 90 days (airflow/dags/agoranomos_pipelines/maintenance.py). dbt only makes the
-- table, empty, with its columns, as elementary makes its own: a run adds no row, a column that a
-- later release declares is added, and full_refresh false keeps the rows from dbt run
-- --full-refresh.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    full_refresh=false,
    on_schema_change='append_new_columns',
) }}

select
    cast(null as timestamp with time zone) as measured_at,
    cast(null as text) as maintenance_run_id,
    cast(null as bigint) as disk_bytes,
    cast(null as bigint) as disk_free_bytes,
    cast(null as bigint) as warehouse_bytes,
    cast(null as bigint) as core_bytes,
    cast(null as bigint) as window_bytes,
    cast(null as bigint) as window_free_bytes
where false
