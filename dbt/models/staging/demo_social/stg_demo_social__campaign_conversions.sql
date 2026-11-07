-- stg_demo_social__campaign_conversions.sql: its conversions by campaign, day and action type.
{{ meta_conversions(newest_load(demo_source('raw_demo_social', 'insights_campaign'), 'raw_demo_social', ref('stg_loads__newest_days')), demo_source('raw_demo_social', 'insights_campaign__actions'), demo_source('raw_demo_social', 'insights_campaign__action_values'), 'demo_social') }}
