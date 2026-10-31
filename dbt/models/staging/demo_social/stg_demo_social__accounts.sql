-- stg_demo_social__accounts.sql: its ad accounts.
-- depends_on: {{ ref('seed_iso_4217_currencies') }}
-- depends_on: {{ ref('seed_iana_time_zones') }}
{{ meta_accounts(demo_source('raw_demo_social', 'ad_accounts'), 'demo_social') }}
