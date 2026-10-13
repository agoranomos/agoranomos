-- stg_demo_video__accounts.sql: its ad accounts, the advertisers.
{{ tiktok_accounts(source('raw_demo_video', 'advertisers'), 'demo_video') }}
