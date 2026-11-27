-- stg_meta_ads__accounts.sql: the accounts that clients.toml names, from the newest load of each.
{{ meta_accounts(newest_list(relation_or_empty(source('raw_meta_ads', 'ad_accounts'), {
    'id': 'text', 'account_id': 'text', 'name': 'text', 'currency': 'text', 'timezone_name': 'text',
    'account_status': 'text', '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_meta_ads', ref('stg_loads__newest_lists')), 'meta_ads') }}
