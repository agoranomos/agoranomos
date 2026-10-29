-- stg_demo_video__ad_groups.sql: the versions of its ad groups.
{{ tiktok_ad_groups(source('raw_demo_video', 'adgroups'), 'demo_video') }}
