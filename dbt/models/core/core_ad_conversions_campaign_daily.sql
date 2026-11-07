-- core.ad_conversions_campaign_daily, the conversions of the campaigns of every ad platform by
-- day and conversion action (ADR-0036, ADR-0038): each run replaces the days of the windows that
-- it pinned whole, by account and day, with the rows of the newest load of each day, in one
-- transaction (macros/facts.sql). It has no journal of restatements.
{{ config(
    alias='ad_conversions_campaign_daily',
    incremental_strategy='delete+insert',
    unique_key=['account_sk', 'report_date'],
    incremental_predicates=['DBT_INTERNAL_DEST.report_date >= ' ~ window_start()],
    pre_hook=[
        "{{ fact_thaw(ref('int_window_days')) }}",
        "{{ fact_empty_days(fact_conversions_rows([ref('stg_demo_search__campaign_conversions'), ref('stg_demo_social__campaign_conversions'), ref('stg_demo_video__campaign_conversions'), ref('stg_google_ads__campaign_conversions')], ref('int_window_days'), ref('dim_conversion_action')), ref('int_window_days')) }}",
    ],
) }}
-- The tests of G2 read the dimension of its entities, which dbt orders before it:
-- depends_on: {{ ref('dim_campaign') }}
{{ fact_conversions_rows([ref('stg_demo_search__campaign_conversions'), ref('stg_demo_social__campaign_conversions'), ref('stg_demo_video__campaign_conversions'), ref('stg_google_ads__campaign_conversions')], ref('int_window_days'), ref('dim_conversion_action')) }}
