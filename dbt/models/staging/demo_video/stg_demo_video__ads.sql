-- stg_demo_video__ads.sql: its ads, as the newest list of each account gives them.
{{ tiktok_ads(newest_list(demo_source('raw_demo_video', 'ads'), 'raw_demo_video', source('core', 'loads')), 'demo_video') }}
