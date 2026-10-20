-- stg_demo_video__campaigns.sql: the versions of its campaigns.
{{ tiktok_campaigns(source('raw_demo_video', 'campaigns'), 'demo_video') }}
