-- stg_google_ads__ad_conversions.sql: the conversions of its ads by day and action.
{{ google_ads_conversions(relation_or_empty(source('raw_google_ads', 'ad_conversions'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'ad_group__resource_name': 'text', 'ad_group__id': 'text', 'ad_group_ad__resource_name': 'text',
    'ad_group_ad__ad__resource_name': 'text', 'ad_group_ad__ad__id': 'text',
    'ad_group_ad__ad__name': 'text', 'segments__date': 'date',
    'segments__conversion_action': 'text', 'segments__conversion_action_name': 'text',
    'segments__conversion_action_category': 'text', 'metrics__conversions': 'double precision',
    'metrics__conversions_value': 'text',
    'metrics__all_conversions': 'double precision',
    'metrics__all_conversions_value': 'text',
}), 'google_ads', 'ad') }}
