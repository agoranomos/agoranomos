-- stg_loads__newest_days.sql: the newest pinned load of each account and day of each source.
-- A table that each run of the transform fills anew from core.loads, which the task of the pin
-- wrote before the tasks of dbt (pin_loads), in one transaction, and analyzes, so that the planner
-- knows its rows when a staging view of a fact joins it (newest_load). core.loads changes only at
-- the pin, so the table holds the same rows however many times a run, or dbt by hand, builds it.
-- Incremental, with its rows deleted first, so that the table stays the same relation, and the
-- views over it stay.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='sync_all_columns',
    pre_hook=["{% if is_incremental() %}delete from {{ this }}{% endif %}"],
    post_hook=["analyze {{ this }}"],
) }}
{%- for source_name in newest_load_sources() %}
{%- if not loop.first %}
union all
{%- endif %}
{{ pinned_loads(source_name, source('core', 'loads')) }}
{%- endfor %}
