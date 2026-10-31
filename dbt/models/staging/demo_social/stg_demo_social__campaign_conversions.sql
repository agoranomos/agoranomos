-- stg_demo_social__campaign_conversions.sql: its conversions by campaign, day and action type.
{{ meta_conversions(demo_source('raw_demo_social', 'insights_campaign'), demo_source('raw_demo_social', 'insights_campaign__actions'), demo_source('raw_demo_social', 'insights_campaign__action_values'), 'demo_social') }}
