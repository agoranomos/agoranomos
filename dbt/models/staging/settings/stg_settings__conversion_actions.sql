-- stg_settings__conversion_actions.sql: the conversion actions that the deployment names.
select platform, conversion_action_id, is_primary, conversion_category, origin
from {{ source('raw_settings', 'conversion_actions') }}
