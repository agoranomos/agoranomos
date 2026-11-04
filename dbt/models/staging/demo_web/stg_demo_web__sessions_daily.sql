-- stg_demo_web__sessions_daily.sql: their sessions by day, source, medium and campaign.
{{ ga4_sessions_daily(newest_load(demo_source('raw_demo_web', 'report_sessions_daily'), 'raw_demo_web', ref('stg_loads__newest_days')), 'demo_web') }}
