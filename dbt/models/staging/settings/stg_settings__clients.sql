-- stg_settings__clients.sql: the clients that the settings of the deployment and of the demo name.
select client_id, client_name, origin
from {{ source('raw_settings', 'clients') }}
