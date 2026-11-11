-- stg_settings__clients.sql: the clients that the settings of the deployment and of the demo name;
-- none before the first run of the transform loads them, as when marts-init builds the views of a
-- new warehouse.
select client_id, client_name, origin
from {{ relation_or_empty(source('raw_settings', 'clients'), {
    'client_id': 'text', 'client_name': 'text', 'origin': 'text',
}) }}
