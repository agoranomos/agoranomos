-- stg_demo_social__ads.sql: its ads, as the newest list of each account gives them.
{{ meta_ads(newest_list(demo_source('raw_demo_social', 'ads'), 'raw_demo_social', ref('stg_loads__newest_lists')), 'demo_social') }}
