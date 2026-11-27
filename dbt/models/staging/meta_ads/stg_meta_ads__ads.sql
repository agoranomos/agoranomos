-- stg_meta_ads__ads.sql: its ads, as the newest list of each account gives them.
{{ meta_ads(newest_list(relation_or_empty(source('raw_meta_ads', 'ads'), {
    'id': 'text', 'account_id': 'text', 'campaign_id': 'text', 'adset_id': 'text', 'name': 'text',
    'status': 'text', 'configured_status': 'text', 'effective_status': 'text',
    '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_meta_ads', ref('stg_loads__newest_lists')), 'meta_ads') }}
