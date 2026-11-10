-- stg_settings__account_clients.sql: the accounts that the settings name, with the client of each;
-- none before the first run of the transform loads them.
select platform, account_id, client_id, origin
from {{ relation_or_empty(source('raw_settings', 'account_clients'), {
    'platform': 'text', 'account_id': 'text', 'client_id': 'text', 'origin': 'text',
}) }}
