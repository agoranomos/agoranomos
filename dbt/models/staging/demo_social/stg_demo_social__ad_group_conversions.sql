-- stg_demo_social__ad_group_conversions.sql: the conversions of its ad groups by day and action.
{{ meta_conversions(source('raw_demo_social', 'insights_adset'), source('raw_demo_social', 'insights_adset__actions'), source('raw_demo_social', 'insights_adset__action_values'), 'demo_social', 'ad_group') }}
