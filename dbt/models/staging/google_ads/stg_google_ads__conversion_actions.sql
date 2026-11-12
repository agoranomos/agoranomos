-- stg_google_ads__conversion_actions.sql: its conversion actions, as the newest list of each account gives them.
{{ google_ads_conversion_actions(newest_list(relation_or_empty(source('raw_google_ads', 'conversion_actions'), {
    'customer__id': 'text', 'conversion_action__resource_name': 'text',
    'conversion_action__id': 'text', 'conversion_action__name': 'text',
    'conversion_action__status': 'text', 'conversion_action__category': 'text',
    'conversion_action__include_in_conversions_metric': 'boolean',
    'conversion_action__primary_for_goal': 'boolean',
    '_dlt_load_id': 'text',
}), 'raw_google_ads', source('core', 'loads')), 'google_ads') }}
