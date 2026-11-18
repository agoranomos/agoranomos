-- stg_meta_ads__campaign_conversions.sql: its conversions by campaign, day and action type.
{{ meta_conversions(newest_load(relation_or_empty(source('raw_meta_ads', 'insights_campaign'), {
    'account_id': 'text', 'account_name': 'text', 'account_currency': 'text', 'campaign_id': 'text',
    'campaign_name': 'text', 'date_start': 'date', 'date_stop': 'date', 'impressions': 'text',
    'clicks': 'text', 'inline_link_clicks': 'text', 'spend': 'text', 'attribution_setting': 'text',
    '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_meta_ads', ref('stg_loads__newest_days')),
relation_or_empty(source('raw_meta_ads', 'insights_campaign__actions'), {
    'action_type': 'text', 'value': 'text', '_dlt_parent_id': 'text', '_dlt_id': 'text',
}),
relation_or_empty(source('raw_meta_ads', 'insights_campaign__action_values'), {
    'action_type': 'text', 'value': 'text', '_dlt_parent_id': 'text', '_dlt_id': 'text',
}), 'meta_ads') }}
