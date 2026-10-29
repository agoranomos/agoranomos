-- stg_google_ads__campaigns.sql: the versions of its campaigns.
{{ google_ads_campaigns(relation_or_empty(source('raw_google_ads', 'campaigns'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'campaign__name': 'text', 'campaign__status': 'text',
    'campaign__advertising_channel_type': 'text', '_dlt_valid_from': 'timestamp with time zone',
    '_dlt_valid_to': 'timestamp with time zone',
}), 'google_ads') }}
