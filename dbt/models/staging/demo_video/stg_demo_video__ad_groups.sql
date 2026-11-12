-- stg_demo_video__ad_groups.sql: its ad groups, as the newest list of each account gives them.
{{ tiktok_ad_groups(newest_list(demo_source('raw_demo_video', 'adgroups'), 'raw_demo_video', source('core', 'loads')), 'demo_video') }}
