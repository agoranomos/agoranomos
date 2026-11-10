-- stg_demo_video__accounts.sql: its ad accounts, the advertisers, as the newest load of each gives them.
{{ tiktok_accounts(newest_list(demo_source('raw_demo_video', 'advertisers'), 'raw_demo_video', source('core', 'loads')), 'demo_video') }}
