-- stg_demo_social__ad_groups.sql: its ad groups, as the newest list of each account gives them.
{{ meta_ad_groups(newest_list(demo_source('raw_demo_social', 'adsets'), 'raw_demo_social', source('core', 'loads')), 'demo_social') }}
