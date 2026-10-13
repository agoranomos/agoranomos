-- stg_demo_social__accounts.sql: its ad accounts.
{{ meta_accounts(source('raw_demo_social', 'ad_accounts'), 'demo_social') }}
