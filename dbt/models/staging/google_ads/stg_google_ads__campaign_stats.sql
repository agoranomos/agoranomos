-- stg_google_ads__campaign_stats.sql: the statistics of the campaigns of google_ads by day.
{{ google_ads_stats(relation_or_empty(source('raw_google_ads', 'campaign_stats'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'campaign__name': 'text', 'metrics__impressions': 'text', 'metrics__clicks': 'text',
    'metrics__cost_micros': 'text', 'metrics__conversions': 'double precision',
    'metrics__conversions_value': 'text', 'segments__date': 'date',
}), 'google_ads') }}
