-- stg_tiktok_ads__campaigns.sql: its campaigns, as the newest list of each advertiser gives them.
{{ tiktok_campaigns(newest_list(relation_or_empty(source('raw_tiktok_ads', 'campaigns'), {
    'advertiser_id': 'text', 'campaign_id': 'text', 'campaign_name': 'text',
    'objective_type': 'text', 'operation_status': 'text', 'secondary_status': 'text',
    '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_tiktok_ads', ref('stg_loads__newest_lists')), 'tiktok_ads') }}
