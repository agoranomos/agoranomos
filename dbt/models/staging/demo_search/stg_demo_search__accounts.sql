-- stg_demo_search__accounts.sql: its ad accounts, without the manager account.
{{ google_ads_accounts(source('raw_demo_search', 'customer_clients'), 'demo_search') }}
