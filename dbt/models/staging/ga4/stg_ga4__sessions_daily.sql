-- stg_ga4__sessions_daily.sql: their sessions by day, source, medium and campaign.
{{ ga4_sessions_daily(newest_load(relation_or_empty(source('raw_ga4', 'report_sessions_daily'), {
    'property': 'text', 'date': 'text', 'session_source': 'text', 'session_medium': 'text',
    'session_campaign_name': 'text', 'session_campaign_id': 'text',
    'session_default_channel_group': 'text', 'sessions': 'text', 'engaged_sessions': 'text',
    'key_events': 'text', 'total_revenue': 'text', '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_ga4', ref('stg_loads__newest_days')), 'ga4') }}
