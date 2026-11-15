-- stg_meta_ads__ad_groups.sql: its ad sets, as the newest list of each account gives them.
{{ meta_ad_groups(newest_list(relation_or_empty(source('raw_meta_ads', 'adsets'), {
    'id': 'text', 'account_id': 'text', 'campaign_id': 'text', 'name': 'text', 'status': 'text',
    'configured_status': 'text', 'effective_status': 'text', 'optimization_goal': 'text',
    '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_meta_ads', source('core', 'loads')), 'meta_ads') }}
