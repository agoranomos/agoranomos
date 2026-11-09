-- stg_google_ads__account_totals.sql: the totals of each account by day, for G0a.
{{ google_ads_totals(newest_load(relation_or_empty(source('raw_google_ads', 'customer_stats'), {
    'customer__id': 'text', 'metrics__impressions': 'text', 'metrics__clicks': 'text',
    'metrics__cost_micros': 'text', 'segments__date': 'date',
    '_dlt_load_id': 'text',
}), 'raw_google_ads', ref('stg_loads__newest_days')), 'google_ads') }}
