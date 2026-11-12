-- stg_google_ads__ads.sql: its ads, as the newest list of each account gives them.
{{ google_ads_ads(newest_list(relation_or_empty(source('raw_google_ads', 'ads'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'ad_group__resource_name': 'text', 'ad_group__id': 'text', 'ad_group_ad__resource_name': 'text',
    'ad_group_ad__status': 'text', 'ad_group_ad__ad__resource_name': 'text',
    'ad_group_ad__ad__id': 'text', 'ad_group_ad__ad__name': 'text', 'ad_group_ad__ad__type': 'text',
    '_dlt_load_id': 'text',
}), 'raw_google_ads', source('core', 'loads')), 'google_ads') }}
