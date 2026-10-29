-- stg_google_ads__ads.sql: the versions of its ads.
{{ google_ads_ads(relation_or_empty(source('raw_google_ads', 'ads'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'ad_group__resource_name': 'text', 'ad_group__id': 'text', 'ad_group_ad__resource_name': 'text',
    'ad_group_ad__status': 'text', 'ad_group_ad__ad__resource_name': 'text',
    'ad_group_ad__ad__id': 'text', 'ad_group_ad__ad__name': 'text', 'ad_group_ad__ad__type': 'text',
    '_dlt_valid_from': 'timestamp with time zone', '_dlt_valid_to': 'timestamp with time zone',
}), 'google_ads') }}
