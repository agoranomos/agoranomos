-- stg_google_ads__ad_groups.sql: its ad groups, as the newest list of each account gives them.
{{ google_ads_ad_groups(newest_list(relation_or_empty(source('raw_google_ads', 'ad_groups'), {
    'customer__id': 'text', 'campaign__resource_name': 'text', 'campaign__id': 'text',
    'ad_group__resource_name': 'text', 'ad_group__id': 'text', 'ad_group__name': 'text',
    'ad_group__status': 'text', 'ad_group__type': 'text',
    '_dlt_load_id': 'text',
}), 'raw_google_ads', source('core', 'loads')), 'google_ads') }}
