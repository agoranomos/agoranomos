-- The monthly comparison of the core with the totals that each platform gives for an account and a
-- day, core.platform_totals_daily (ADR-0039): the task compare_with_platform_totals of the DAG of
-- maintenance writes one row for each account, and deletes the rows older than 13 months
-- (airflow/dags/agoranomos_pipelines/maintenance.py). dbt only makes the table, empty, with its
-- columns, as elementary makes its own: a run adds no row, a column that a later release declares
-- is added, and full_refresh false keeps the rows from dbt run --full-refresh.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    full_refresh=false,
    on_schema_change='append_new_columns',
) }}

select
    cast(null as timestamp with time zone) as checked_at,
    cast(null as text) as maintenance_run_id,
    cast(null as text) as platform,
    cast(null as text) as account_id,
    cast(null as date) as first_day,
    cast(null as date) as last_day,
    cast(null as bigint) as days,
    cast(null as bigint) as days_differing,
    cast(null as bigint) as platform_impressions,
    cast(null as bigint) as core_impressions,
    cast(null as bigint) as platform_clicks,
    cast(null as bigint) as core_clicks,
    cast(null as numeric(18, 6)) as platform_spend,
    cast(null as numeric(18, 6)) as core_spend
where false
