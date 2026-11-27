-- int_accounts__currencies.sql: the accounts and the properties that the lists of the accounts and
-- of the properties give, with the currency of each (listed_currencies), which ops_rates reads to
-- name the days without a rate of an account in quarantine. A table that each run of the transform
-- fills anew from the staging views of the lists, and not a view over them, as ops_rates is one:
-- dbt replaces a view by dropping the old one with cascade, which took ops_rates with it from
-- early in each run until its own node built it again near the end. Incremental, with its rows
-- deleted first, so that the table stays the same relation, and the views over it stay, as
-- stg_loads__newest_lists does. marts-init builds it with the views of ops at ./agoranomos up, and
-- never a list: a list that the release before lacked, as those of TikTok and GA4 after v0.6.0,
-- gives no account until the first run of the transform builds it (listed_currencies).
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='sync_all_columns',
    pre_hook=["{% if is_incremental() %}delete from {{ this }}{% endif %}"],
) }}
{%- set lists = [
    (ref('stg_demo_search__accounts'), 'account_id'),
    (ref('stg_demo_social__accounts'), 'account_id'),
    (ref('stg_demo_video__accounts'), 'account_id'),
    (ref('stg_google_ads__accounts'), 'account_id'),
    (ref('stg_meta_ads__accounts'), 'account_id'),
    (ref('stg_tiktok_ads__accounts'), 'account_id'),
    (ref('stg_demo_web__properties'), 'property_id'),
    (ref('stg_ga4__properties'), 'property_id'),
] %}
select distinct
    listed.source::text as source,
    listed.account_id::text as account_id,
    listed.currency::text as currency
from ({{ listed_currencies(lists) }}) as listed
