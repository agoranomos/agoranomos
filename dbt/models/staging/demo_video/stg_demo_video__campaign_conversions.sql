-- stg_demo_video__campaign_conversions.sql: its conversions by campaign, day and action.
{{ tiktok_conversions(newest_load(demo_source('raw_demo_video', 'report_campaign_daily'), 'raw_demo_video', ref('stg_loads__newest_days')), 'demo_video') }}
