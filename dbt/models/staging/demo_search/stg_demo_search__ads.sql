-- stg_demo_search__ads.sql: the versions of its ads.
{{ google_ads_ads(source('raw_demo_search', 'ads'), 'demo_search') }}
