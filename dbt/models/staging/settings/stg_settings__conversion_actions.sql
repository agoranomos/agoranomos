-- stg_settings__conversion_actions.sql: the conversion actions that the deployment names; none
-- before the first run of the transform loads them.
select platform, conversion_action_id, is_primary, conversion_category, origin
from {{ relation_or_empty(source('raw_settings', 'conversion_actions'), {
    'platform': 'text', 'conversion_action_id': 'text', 'is_primary': 'boolean',
    'conversion_category': 'text', 'origin': 'text',
}) }}
