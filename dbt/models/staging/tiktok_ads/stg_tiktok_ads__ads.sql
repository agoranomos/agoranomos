-- stg_tiktok_ads__ads.sql: its ads, as the newest list of each advertiser gives them.
{{ tiktok_ads(newest_list(relation_or_empty(source('raw_tiktok_ads', 'ads'), {
    'advertiser_id': 'text', 'campaign_id': 'text', 'adgroup_id': 'text', 'ad_id': 'text',
    'ad_name': 'text', 'ad_format': 'text', 'operation_status': 'text', 'secondary_status': 'text',
    '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_tiktok_ads', ref('stg_loads__newest_lists')), 'tiktok_ads') }}
