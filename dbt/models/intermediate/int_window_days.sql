-- The days of the windows that this run of the transform writes into the facts of the core
-- (ADR-0038): for each account of the ad platforms and each property of web analytics and each day
-- of the loads that the run pinned, the newest load that covers the day, from
-- stg_loads__newest_days, with the surrogate and the currency of the account from core.dim_account
-- and the surrogate of the load from core.loads, beside its ID. A model of a fact replaces each of
-- these days whole: the rows of its load go in, and a day on which the load holds no row is emptied
-- (macros/facts.sql). An account that the checks of this run hold, each account of int_g1_failures,
-- a source that stops among them, is left out here, so that every fact keeps its days as they were
-- (ADR-0035). Only a pinned load counts: once the run accepts its loads, their days leave the
-- table (agoranomos_pipelines/acceptance.py) and raw their rows, so that a build by hand after the
-- run, from stg_loads__newest_days of the run, gives no day that would empty a day of the core.
-- A table that each run fills anew, once, with its statistics, as each model of a fact reads it in
-- three statements: incremental, with its rows deleted first, so that the table stays the same
-- relation. The rows of staging join it on day_key, the source, the account and the day in one
-- column, which the planner estimates from the statistics of that column: on the three columns, it
-- took the join for one row and compared each row of a fact with every row of staging.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='sync_all_columns',
    pre_hook=["{% if is_incremental() %}delete from {{ this }}{% endif %}"],
    post_hook=["analyze {{ this }}"],
) }}
select
    d.source::text as source,
    d.account_id::text as account_id,
    a.account_sk::integer as account_sk,
    a.currency::char(3) as currency,
    d.day::date as report_date,
    l.load_sk::integer as load_sk,
    d.load_id::text as load_id,
    {{ window_day_key('d.source', 'd.account_id', 'd.day') }} as day_key
from {{ ref('stg_loads__newest_days') }} as d
join {{ ref('dim_account') }} as a
    on a.platform = d.source and a.account_id = d.account_id
join {{ source('core', 'loads') }} as l
    on l.source = d.source and l.load_id = d.load_id and l.account_id = d.account_id
    and l.status = 'pinned'
where {{ not_in_quarantine(ref('int_g1_failures'), 'd.source', 'd.account_id') }}
