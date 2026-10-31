-- stg_demo_video__ad_group_conversions.sql: the conversions of its ad groups by day and action.
{{ tiktok_conversions(demo_source('raw_demo_video', 'report_adgroup_daily'), 'demo_video', 'ad_group') }}
