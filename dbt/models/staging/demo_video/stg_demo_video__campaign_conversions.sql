-- stg_demo_video__campaign_conversions.sql: its conversions by campaign, day and action.
{{ tiktok_campaign_conversions(source('raw_demo_video', 'report_campaign_daily'), 'demo_video') }}
