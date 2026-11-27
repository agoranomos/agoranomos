-- stg_ga4__properties.sql: the properties that clients.toml names, from the newest load of each.
{{ ga4_properties(newest_list(relation_or_empty(source('raw_ga4', 'properties'), {
    'name': 'text', 'display_name': 'text', 'time_zone': 'text', 'currency_code': 'text',
    '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_ga4', ref('stg_loads__newest_lists'), "split_part(r.name, '/', 2)"), 'ga4') }}
