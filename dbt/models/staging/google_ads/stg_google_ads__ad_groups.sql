-- stg_google_ads__ad_groups.sql: the versions of its ad groups.
{{ google_ads_ad_groups(relation_or_empty(source('raw_google_ads', 'ad_groups'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'ad_group__resource_name': 'text', 'ad_group__id': 'text', 'ad_group__name': 'text',
    'ad_group__status': 'text', 'ad_group__type': 'text',
    '_dlt_valid_from': 'timestamp with time zone', '_dlt_valid_to': 'timestamp with time zone',
}), 'google_ads') }}
