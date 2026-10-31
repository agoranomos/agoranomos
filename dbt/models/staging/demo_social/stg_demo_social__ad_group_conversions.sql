-- stg_demo_social__ad_group_conversions.sql: the conversions of its ad groups by day and action.
{{ meta_conversions(demo_source('raw_demo_social', 'insights_adset'), demo_source('raw_demo_social', 'insights_adset__actions'), demo_source('raw_demo_social', 'insights_adset__action_values'), 'demo_social', 'ad_group') }}
