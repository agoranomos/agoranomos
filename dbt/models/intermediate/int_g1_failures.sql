-- The checks of G0a, G1 and G1a of ADR-0035 on the data that this run of the transform reads,
-- before it writes the core: one row for each load, account, reason and kind of data that failed.
-- An account with a row here is in quarantine for this run: int_accounts__current, and so every
-- dimension of the core, and the marts of the old path leave it out, core.account_quarantine
-- records it, and a source with more than half of its pinned accounts here stops whole
-- (macros/quarantine.sql). A table that each run fills anew, one statement for each kind of data:
-- as one statement, the checks of three levels held a dozen aggregates at once, and were the
-- largest process of the database (#191), and every dimension, and the facts of #199, read the
-- accounts that it holds. Incremental, with its rows deleted first, so that the table stays the
-- same relation, and the views over it stay; its statistics come last, for the filters that read
-- it.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='sync_all_columns',
    pre_hook=["{% if is_incremental() %}delete from {{ this }}{% endif %}"],
    post_hook=[
        "{{ quarantine_insert(this, quarantine_attributed(quarantine_fact_findings('campaign_stats', [ref('stg_demo_search__campaign_stats'), ref('stg_demo_social__campaign_stats'), ref('stg_demo_video__campaign_stats'), ref('stg_google_ads__campaign_stats')], ref('stg_loads__newest_days')), ref('stg_loads__newest_days'), source('core', 'loads'))) }}",
        "{{ quarantine_insert(this, quarantine_attributed(quarantine_fact_findings('ad_group_stats', [ref('stg_demo_search__ad_group_stats'), ref('stg_demo_social__ad_group_stats'), ref('stg_demo_video__ad_group_stats'), ref('stg_google_ads__ad_group_stats')], ref('stg_loads__newest_days')), ref('stg_loads__newest_days'), source('core', 'loads'))) }}",
        "{{ quarantine_insert(this, quarantine_attributed(quarantine_fact_findings('ad_stats', [ref('stg_demo_search__ad_stats'), ref('stg_demo_social__ad_stats'), ref('stg_demo_video__ad_stats'), ref('stg_google_ads__ad_stats')], ref('stg_loads__newest_days')), ref('stg_loads__newest_days'), source('core', 'loads'))) }}",
        "{{ quarantine_insert(this, quarantine_attributed(quarantine_fact_findings('campaign_conversions', [ref('stg_demo_search__campaign_conversions'), ref('stg_demo_social__campaign_conversions'), ref('stg_demo_video__campaign_conversions'), ref('stg_google_ads__campaign_conversions')], ref('stg_loads__newest_days')), ref('stg_loads__newest_days'), source('core', 'loads'))) }}",
        "{{ quarantine_insert(this, quarantine_attributed(quarantine_fact_findings('ad_group_conversions', [ref('stg_demo_search__ad_group_conversions'), ref('stg_demo_social__ad_group_conversions'), ref('stg_demo_video__ad_group_conversions'), ref('stg_google_ads__ad_group_conversions')], ref('stg_loads__newest_days')), ref('stg_loads__newest_days'), source('core', 'loads'))) }}",
        "{{ quarantine_insert(this, quarantine_attributed(quarantine_fact_findings('ad_conversions', [ref('stg_demo_search__ad_conversions'), ref('stg_demo_social__ad_conversions'), ref('stg_demo_video__ad_conversions'), ref('stg_google_ads__ad_conversions')], ref('stg_loads__newest_days')), ref('stg_loads__newest_days'), source('core', 'loads'))) }}",
        "{{ quarantine_insert(this, quarantine_attributed(quarantine_fact_findings('sessions_daily', [ref('stg_demo_web__sessions_daily')], ref('stg_loads__newest_days')), ref('stg_loads__newest_days'), source('core', 'loads'))) }}",
        "{% for source_name in quarantine_raw_tables() %}{{ quarantine_insert(this, quarantine_attributed(quarantine_raw_findings(source_name, ref('stg_loads__newest_days'), source('core', 'loads')), ref('stg_loads__newest_days'), source('core', 'loads'))) }};{% endfor %}",
        "{{ quarantine_insert(this, quarantine_attributed(quarantine_totals_findings([(ref('stg_google_ads__campaign_stats'), ref('stg_google_ads__account_totals'))], ref('stg_loads__newest_days')), ref('stg_loads__newest_days'), source('core', 'loads'))) }}",
        "{{ quarantine_insert(this, quarantine_attributed(quarantine_collapse_findings([ref('stg_demo_search__campaign_stats'), ref('stg_demo_social__campaign_stats'), ref('stg_demo_video__campaign_stats'), ref('stg_google_ads__campaign_stats')], ref('stg_loads__newest_days'), source('core', 'dim_account'), source('core', 'ad_stats_campaign_daily')), ref('stg_loads__newest_days'), source('core', 'loads'))) }}",
        "{{ quarantine_insert(this, quarantine_attributed(quarantine_collapse_findings([ref('stg_demo_web__sessions_daily')], ref('stg_loads__newest_days'), source('core', 'dim_account'), source('core', 'web_sessions_daily'), measure='sessions', checked='sessions_daily', account='property_id', fall=false), ref('stg_loads__newest_days'), source('core', 'loads'))) }}",
        "{{ quarantine_insert(this, quarantine_attributed(quarantine_stop_findings(this, source('core', 'loads')), ref('stg_loads__newest_days'), source('core', 'loads'))) }}",
        "analyze {{ this }}",
    ],
) }}
-- The statements of the hooks read these, which dbt orders before this model:
-- depends_on: {{ ref('stg_demo_search__campaign_stats') }}
-- depends_on: {{ ref('stg_demo_social__campaign_stats') }}
-- depends_on: {{ ref('stg_demo_video__campaign_stats') }}
-- depends_on: {{ ref('stg_google_ads__campaign_stats') }}
-- depends_on: {{ ref('stg_demo_search__ad_group_stats') }}
-- depends_on: {{ ref('stg_demo_social__ad_group_stats') }}
-- depends_on: {{ ref('stg_demo_video__ad_group_stats') }}
-- depends_on: {{ ref('stg_google_ads__ad_group_stats') }}
-- depends_on: {{ ref('stg_demo_search__ad_stats') }}
-- depends_on: {{ ref('stg_demo_social__ad_stats') }}
-- depends_on: {{ ref('stg_demo_video__ad_stats') }}
-- depends_on: {{ ref('stg_google_ads__ad_stats') }}
-- depends_on: {{ ref('stg_demo_search__campaign_conversions') }}
-- depends_on: {{ ref('stg_demo_social__campaign_conversions') }}
-- depends_on: {{ ref('stg_demo_video__campaign_conversions') }}
-- depends_on: {{ ref('stg_google_ads__campaign_conversions') }}
-- depends_on: {{ ref('stg_demo_search__ad_group_conversions') }}
-- depends_on: {{ ref('stg_demo_social__ad_group_conversions') }}
-- depends_on: {{ ref('stg_demo_video__ad_group_conversions') }}
-- depends_on: {{ ref('stg_google_ads__ad_group_conversions') }}
-- depends_on: {{ ref('stg_demo_search__ad_conversions') }}
-- depends_on: {{ ref('stg_demo_social__ad_conversions') }}
-- depends_on: {{ ref('stg_demo_video__ad_conversions') }}
-- depends_on: {{ ref('stg_google_ads__ad_conversions') }}
-- depends_on: {{ ref('stg_demo_web__sessions_daily') }}
-- depends_on: {{ ref('stg_google_ads__account_totals') }}
-- depends_on: {{ source('core', 'dim_account') }}
-- depends_on: {{ source('core', 'ad_stats_campaign_daily') }}
-- depends_on: {{ source('core', 'web_sessions_daily') }}
{{ quarantine_attributed(
    quarantine_account_findings(
        [
            (ref('stg_demo_search__accounts'), 'account_id'),
            (ref('stg_demo_social__accounts'), 'account_id'),
            (ref('stg_demo_video__accounts'), 'account_id'),
            (ref('stg_google_ads__accounts'), 'account_id'),
            (ref('stg_demo_web__properties'), 'property_id'),
        ],
        ref('seed_iso_4217_currencies'),
        ref('seed_iana_time_zones'),
        ref('stg_loads__newest_days'),
        source('core', 'loads'),
    ),
    ref('stg_loads__newest_days'),
    source('core', 'loads'),
) }}
