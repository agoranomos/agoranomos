-- stg_google_ads__conversion_actions.sql: the versions of its conversion actions.
{{ google_ads_conversion_actions(relation_or_empty(source('raw_google_ads', 'conversion_actions'), {
    'customer__id': 'text', 'conversion_action__resource_name': 'text',
    'conversion_action__id': 'text', 'conversion_action__name': 'text',
    'conversion_action__status': 'text', 'conversion_action__category': 'text',
    'conversion_action__include_in_conversions_metric': 'boolean',
    'conversion_action__primary_for_goal': 'boolean',
    '_dlt_valid_from': 'timestamp with time zone', '_dlt_valid_to': 'timestamp with time zone',
}), 'google_ads') }}
