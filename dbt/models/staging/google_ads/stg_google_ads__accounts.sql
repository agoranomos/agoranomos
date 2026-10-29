-- stg_google_ads__accounts.sql: the accounts that clients.toml names, without a manager account.
-- depends_on: {{ ref('seed_iso_4217_currencies') }}
-- depends_on: {{ ref('seed_iana_time_zones') }}
{{ google_ads_customers(relation_or_empty(source('raw_google_ads', 'customers'), {
    'customer__resource_name': 'text', 'customer__id': 'text',
    'customer__descriptive_name': 'text', 'customer__currency_code': 'text',
    'customer__time_zone': 'text', 'customer__manager': 'boolean', 'customer__status': 'text',
}), 'google_ads') }}
