-- stg_settings__account_clients.sql: the accounts that the settings name, with the client of each.
select platform, account_id, client_id, origin
from {{ source('raw_settings', 'account_clients') }}
