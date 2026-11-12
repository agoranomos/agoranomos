-- stg_google_ads__campaigns.sql: its campaigns, as the newest list of each account gives them.
{{ google_ads_campaigns(newest_list(relation_or_empty(source('raw_google_ads', 'campaigns'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'campaign__name': 'text', 'campaign__status': 'text',
    'campaign__advertising_channel_type': 'text', '_dlt_load_id': 'text',
}), 'raw_google_ads', source('core', 'loads')), 'google_ads') }}
