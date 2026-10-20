-- stg_demo_video__accounts.sql: its ad accounts, the advertisers.
-- depends_on: {{ ref('seed_iso_4217_currencies') }}
-- depends_on: {{ ref('seed_iana_time_zones') }}
{{ tiktok_accounts(source('raw_demo_video', 'advertisers'), 'demo_video') }}
