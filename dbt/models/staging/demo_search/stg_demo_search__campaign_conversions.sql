-- stg_demo_search__campaign_conversions.sql: its conversions by campaign, day and action.
{{ google_ads_conversions(newest_load(demo_source('raw_demo_search', 'campaign_conversions'), 'raw_demo_search', ref('stg_loads__newest_days')), 'demo_search') }}
