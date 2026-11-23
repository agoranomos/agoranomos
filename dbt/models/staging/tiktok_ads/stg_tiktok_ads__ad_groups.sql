-- stg_tiktok_ads__ad_groups.sql: its ad groups, as the newest list of each advertiser gives them.
{{ tiktok_ad_groups(newest_list(relation_or_empty(source('raw_tiktok_ads', 'adgroups'), {
    'advertiser_id': 'text', 'campaign_id': 'text', 'adgroup_id': 'text', 'adgroup_name': 'text',
    'optimization_goal': 'text', 'operation_status': 'text', 'secondary_status': 'text',
    '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_tiktok_ads', source('core', 'loads')), 'tiktok_ads') }}
