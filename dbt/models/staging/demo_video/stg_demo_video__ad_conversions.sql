-- stg_demo_video__ad_conversions.sql: the conversions of its ads by day and action.
{{ tiktok_conversions(source('raw_demo_video', 'report_ad_daily'), 'demo_video', 'ad') }}
