-- stg_demo_web__properties.sql: the properties of web analytics of demo_web.
-- depends_on: {{ ref('seed_iso_4217_currencies') }}
-- depends_on: {{ ref('seed_iana_time_zones') }}
{{ ga4_properties(source('raw_demo_web', 'properties'), 'demo_web') }}
