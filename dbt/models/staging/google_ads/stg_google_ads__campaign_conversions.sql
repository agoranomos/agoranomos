-- stg_google_ads__campaign_conversions.sql: its conversions by campaign, day and action.
{{ google_ads_conversions(newest_load(relation_or_empty(source('raw_google_ads', 'campaign_conversions'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'campaign__name': 'text', 'segments__date': 'date', 'segments__conversion_action': 'text',
    'segments__conversion_action_name': 'text', 'segments__conversion_action_category': 'text',
    'metrics__conversions': 'double precision',
    'metrics__conversions_value': 'text',
    'metrics__all_conversions': 'double precision',
    'metrics__all_conversions_value': 'text',
    '_dlt_load_id': 'text',
}), 'raw_google_ads', ref('stg_loads__newest_days')), 'google_ads') }}
