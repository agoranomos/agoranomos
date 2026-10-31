-- stg_demo_search__accounts.sql: its ad accounts, without the manager account.
-- depends_on: {{ ref('seed_iso_4217_currencies') }}
-- depends_on: {{ ref('seed_iana_time_zones') }}
{{ google_ads_accounts(demo_source('raw_demo_search', 'customer_clients'), 'demo_search') }}
