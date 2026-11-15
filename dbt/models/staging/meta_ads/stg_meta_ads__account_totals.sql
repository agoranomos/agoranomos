-- stg_meta_ads__account_totals.sql: the totals of each account by day, for G0a.
{{ meta_totals(newest_load(relation_or_empty(source('raw_meta_ads', 'insights_account'), {
    'account_id': 'text', 'date_start': 'date', 'date_stop': 'date', 'impressions': 'text',
    'inline_link_clicks': 'text', 'spend': 'text', '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_meta_ads', ref('stg_loads__newest_days')), 'meta_ads') }}
