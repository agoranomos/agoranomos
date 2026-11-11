-- stg_google_ads__campaign_stats.sql: the statistics of the campaigns of google_ads by day.
{{ google_ads_stats(newest_load(relation_or_empty(source('raw_google_ads', 'campaign_stats'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'campaign__name': 'text', 'metrics__impressions': 'text', 'metrics__clicks': 'text',
    'metrics__cost_micros': 'text', 'metrics__conversions': 'double precision',
    'metrics__conversions_value': 'text', 'metrics__all_conversions': 'double precision',
    'metrics__all_conversions_value': 'text', 'segments__date': 'date',
    '_dlt_load_id': 'text',
}), 'raw_google_ads', ref('stg_loads__newest_days')), 'google_ads', all_conversions=true) }}
