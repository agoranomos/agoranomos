-- stg_demo_search__ad_group_conversions.sql: the conversions of its ad groups by day and action.
{{ google_ads_conversions(source('raw_demo_search', 'ad_group_conversions'), 'demo_search', 'ad_group') }}
