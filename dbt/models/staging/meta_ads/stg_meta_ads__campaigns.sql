-- stg_meta_ads__campaigns.sql: its campaigns, as the newest list of each account gives them.
{{ meta_campaigns(newest_list(relation_or_empty(source('raw_meta_ads', 'campaigns'), {
    'id': 'text', 'account_id': 'text', 'name': 'text', 'status': 'text',
    'configured_status': 'text', 'effective_status': 'text', 'objective': 'text',
    '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_meta_ads', source('core', 'loads')), 'meta_ads') }}
