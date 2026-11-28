-- int_rates__given.sql: the rates that the deployment gives in extensions/model/rates.toml, as
-- stg_settings__rates gives them, which ops_rates reads to name the rates of the file. A table
-- that each run of the transform fills anew from the staging view, and not the view itself, as
-- ops_rates is a view: dbt replaces a view by dropping the old one with cascade, which took
-- ops_rates with it from early in each run until its own node built it again near the end.
-- Incremental, with its rows deleted first, so that the table stays the same relation, and the
-- views over it stay, as stg_loads__newest_lists does. The model of the rates and the check of
-- the rates read the view, in the run that loads the file.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='sync_all_columns',
    pre_hook=["{% if is_incremental() %}delete from {{ this }}{% endif %}"],
) }}
select currency, quote_currency, rate, first_day, last_day, origin
from {{ ref('stg_settings__rates') }}
