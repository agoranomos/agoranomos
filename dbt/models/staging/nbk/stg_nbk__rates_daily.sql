-- stg_nbk__rates_daily.sql: the official rates of the tenge of the NBK by day.
{{ nbk_rates(newest_load(relation_or_empty(source('raw_nbk', 'rates_daily'), {
    'date': 'text', 'fullname': 'text', 'title': 'text', 'description': 'text', 'quant': 'text',
    'index': 'text', 'change': 'text', '_dlt_load_id': 'text',
}), 'raw_nbk', ref('stg_loads__newest_days')), 'nbk') }}
