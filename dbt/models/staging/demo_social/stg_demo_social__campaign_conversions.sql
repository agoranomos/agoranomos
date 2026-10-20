-- stg_demo_social__campaign_conversions.sql: its conversions by campaign, day and action type.
{{ meta_campaign_conversions(source('raw_demo_social', 'insights_campaign'), source('raw_demo_social', 'insights_campaign__actions'), source('raw_demo_social', 'insights_campaign__action_values'), 'demo_social') }}
