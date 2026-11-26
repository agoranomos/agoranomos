-- stg_demo_social__campaigns.sql: its campaigns, as the newest list of each account gives them.
{{ meta_campaigns(newest_list(demo_source('raw_demo_social', 'campaigns'), 'raw_demo_social', ref('stg_loads__newest_lists')), 'demo_social') }}
