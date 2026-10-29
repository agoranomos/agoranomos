-- stg_demo_social__ad_groups.sql: the versions of its ad groups.
{{ meta_ad_groups(source('raw_demo_social', 'adsets'), 'demo_social') }}
