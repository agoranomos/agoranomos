-- core.ad_stats_campaign_daily, the statistics of the campaigns of every ad platform by day
-- (ADR-0036, ADR-0038): each run replaces the days of the windows that it pinned whole, by account
-- and day, with the rows of the newest load of each day, and keeps the rows that it changes or
-- removes in core.ad_stats_campaign_daily_restatements, in one transaction (macros/facts.sql).
{{ config(
    alias='ad_stats_campaign_daily',
    incremental_strategy='delete+insert',
    unique_key=['account_sk', 'report_date'],
    incremental_predicates=['DBT_INTERNAL_DEST.report_date >= ' ~ window_start()],
    pre_hook=[
        "{{ fact_thaw(ref('int_window_days')) }}",
        "{{ fact_restatements(fact_stats_rows('campaign', [ref('stg_demo_search__campaign_stats'), ref('stg_demo_social__campaign_stats'), ref('stg_demo_video__campaign_stats'), ref('stg_google_ads__campaign_stats')], ref('int_window_days')), ref('int_window_days')) }}",
        "{{ fact_empty_days(fact_stats_rows('campaign', [ref('stg_demo_search__campaign_stats'), ref('stg_demo_social__campaign_stats'), ref('stg_demo_video__campaign_stats'), ref('stg_google_ads__campaign_stats')], ref('int_window_days')), ref('int_window_days')) }}",
    ],
) }}
-- The tests of G2 read the dimension of its entities, which dbt orders before it:
-- depends_on: {{ ref('dim_campaign') }}
{{ fact_stats_rows('campaign', [ref('stg_demo_search__campaign_stats'), ref('stg_demo_social__campaign_stats'), ref('stg_demo_video__campaign_stats'), ref('stg_google_ads__campaign_stats')], ref('int_window_days')) }}
