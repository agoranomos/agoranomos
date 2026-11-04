-- stg_demo_social__ad_group_conversions.sql: the conversions of its ad groups by day and action.
{{ meta_conversions(newest_load(demo_source('raw_demo_social', 'insights_adset'), 'raw_demo_social', ref('stg_loads__newest_days')), demo_source('raw_demo_social', 'insights_adset__actions'), demo_source('raw_demo_social', 'insights_adset__action_values'), 'demo_social', 'ad_group') }}
