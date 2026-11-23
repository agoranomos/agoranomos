-- stg_tiktok_ads__accounts.sql: the advertisers that clients.toml names, from the newest load of each.
{{ tiktok_accounts(newest_list(relation_or_empty(source('raw_tiktok_ads', 'advertisers'), {
    'advertiser_id': 'text', 'name': 'text', 'currency': 'text', 'timezone': 'text',
    'display_timezone': 'text', 'status': 'text', '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_tiktok_ads', source('core', 'loads')), 'tiktok_ads') }}
