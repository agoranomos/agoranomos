-- stg_demo_web__sessions_daily.sql: their sessions by day, source, medium and campaign.
{{ ga4_sessions_daily(source('raw_demo_web', 'report_sessions_daily'), 'demo_web') }}
