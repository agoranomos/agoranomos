-- stg_demo_video__accounts.sql: its ad accounts, the advertisers, as the newest load of each gives them.
{{ tiktok_accounts(newest_list(demo_source('raw_demo_video', 'advertisers'), 'raw_demo_video', ref('stg_loads__newest_lists')), 'demo_video') }}
