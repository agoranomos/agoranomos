-- stg_demo_social__ad_conversions.sql: the conversions of its ads by day and action.
{{ meta_conversions(newest_load(demo_source('raw_demo_social', 'insights_ad'), 'raw_demo_social', ref('stg_loads__newest_days')), demo_source('raw_demo_social', 'insights_ad__actions'), demo_source('raw_demo_social', 'insights_ad__action_values'), 'demo_social', 'ad') }}
