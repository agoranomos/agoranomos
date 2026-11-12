-- stg_demo_social__accounts.sql: its ad accounts, as the newest load of each gives them.
{{ meta_accounts(newest_list(demo_source('raw_demo_social', 'ad_accounts'), 'raw_demo_social', source('core', 'loads')), 'demo_social') }}
