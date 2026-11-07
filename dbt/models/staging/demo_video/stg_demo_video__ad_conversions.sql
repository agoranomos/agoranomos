-- stg_demo_video__ad_conversions.sql: the conversions of its ads by day and action.
{{ tiktok_conversions(newest_load(demo_source('raw_demo_video', 'report_ad_daily'), 'raw_demo_video', ref('stg_loads__newest_days')), 'demo_video', 'ad') }}
