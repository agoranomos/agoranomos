-- stg_meta_ads__ad_group_stats.sql: the statistics of its ad sets, its ad groups, by day.
{{ meta_stats(newest_load(relation_or_empty(source('raw_meta_ads', 'insights_adset'), {
    'account_id': 'text', 'account_name': 'text', 'account_currency': 'text', 'campaign_id': 'text',
    'adset_id': 'text', 'adset_name': 'text', 'date_start': 'date', 'date_stop': 'date',
    'impressions': 'text', 'clicks': 'text', 'inline_link_clicks': 'text', 'spend': 'text',
    'attribution_setting': 'text', '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_meta_ads', ref('stg_loads__newest_days')), 'meta_ads', 'ad_group') }}
