-- core.web_sessions_daily, the sessions of web analytics by day, property, source, medium and
-- campaign (ADR-0036, ADR-0038): each run replaces the days of the windows that it pinned whole,
-- by property and day, with the rows of the newest load of each day, in one transaction
-- (macros/facts.sql). It has no journal of restatements.
{{ config(
    alias='web_sessions_daily',
    incremental_strategy='delete+insert',
    unique_key=['account_sk', 'report_date'],
    incremental_predicates=['DBT_INTERNAL_DEST.report_date >= ' ~ window_start()],
    pre_hook=[
        "{{ fact_thaw(ref('int_window_days')) }}",
        "{{ fact_empty_days(fact_sessions_rows([ref('stg_demo_web__sessions_daily')], ref('int_window_days')), ref('int_window_days')) }}",
    ],
) }}
{{ fact_sessions_rows([ref('stg_demo_web__sessions_daily')], ref('int_window_days')) }}
