-- stg_demo_video__campaigns.sql: its campaigns, as the newest list of each account gives them.
{{ tiktok_campaigns(newest_list(demo_source('raw_demo_video', 'campaigns'), 'raw_demo_video', source('core', 'loads')), 'demo_video') }}
