-- stg_settings__rates.sql: the rates that the deployment gives in extensions/model/rates.toml, one
-- row for each entry, with the rate as numeric, exact as the file writes it; none before the first
-- run of the transform loads them.
select currency, quote_currency, cast(rate as numeric) as rate, first_day, last_day, origin
from {{ relation_or_empty(source('raw_settings', 'rates'), {
    'currency': 'text', 'quote_currency': 'text', 'rate': 'text', 'first_day': 'date',
    'last_day': 'date', 'origin': 'text',
}) }}
