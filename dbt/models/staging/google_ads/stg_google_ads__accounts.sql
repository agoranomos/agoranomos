-- stg_google_ads__accounts.sql: the accounts that clients.toml names, from the newest load of each.
{{ google_ads_customers(newest_list(relation_or_empty(source('raw_google_ads', 'customers'), {
    'customer__resource_name': 'text', 'customer__id': 'text',
    'customer__descriptive_name': 'text', 'customer__currency_code': 'text',
    'customer__time_zone': 'text', 'customer__manager': 'boolean', 'customer__status': 'text',
    '_dlt_load_id': 'text',
}), 'raw_google_ads', source('core', 'loads')), 'google_ads') }}
