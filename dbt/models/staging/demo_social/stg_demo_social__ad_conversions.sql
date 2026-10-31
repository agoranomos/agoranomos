-- stg_demo_social__ad_conversions.sql: the conversions of its ads by day and action.
{{ meta_conversions(demo_source('raw_demo_social', 'insights_ad'), demo_source('raw_demo_social', 'insights_ad__actions'), demo_source('raw_demo_social', 'insights_ad__action_values'), 'demo_social', 'ad') }}
