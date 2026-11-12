-- stg_demo_search__accounts.sql: its ad accounts, not the manager, from the newest load of each.
{{ google_ads_accounts(newest_list(demo_source('raw_demo_search', 'customer_clients'), 'raw_demo_search', source('core', 'loads'), 'r.customer_client__id'), 'demo_search') }}
