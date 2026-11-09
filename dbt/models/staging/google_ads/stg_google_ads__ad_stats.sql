-- stg_google_ads__ad_stats.sql: the statistics of its ads by day.
{{ google_ads_stats(newest_load(relation_or_empty(source('raw_google_ads', 'ad_stats'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'ad_group__resource_name': 'text', 'ad_group__id': 'text', 'ad_group_ad__resource_name': 'text',
    'ad_group_ad__ad__resource_name': 'text', 'ad_group_ad__ad__id': 'text',
    'ad_group_ad__ad__name': 'text', 'metrics__impressions': 'text', 'metrics__clicks': 'text',
    'metrics__cost_micros': 'text', 'metrics__conversions': 'double precision',
    'metrics__conversions_value': 'text', 'metrics__all_conversions': 'double precision',
    'metrics__all_conversions_value': 'text', 'segments__date': 'date',
    '_dlt_load_id': 'text',
}), 'raw_google_ads', ref('stg_loads__newest_days')), 'google_ads', 'ad', all_conversions=true) }}
