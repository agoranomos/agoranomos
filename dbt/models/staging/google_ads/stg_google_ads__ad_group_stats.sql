-- stg_google_ads__ad_group_stats.sql: the statistics of its ad groups by day.
{{ google_ads_stats(relation_or_empty(source('raw_google_ads', 'ad_group_stats'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'ad_group__resource_name': 'text', 'ad_group__id': 'text', 'ad_group__name': 'text',
    'metrics__impressions': 'text', 'metrics__clicks': 'text', 'metrics__cost_micros': 'text',
    'metrics__conversions': 'double precision', 'metrics__conversions_value': 'double precision',
    'segments__date': 'date',
}), 'google_ads', 'ad_group') }}
