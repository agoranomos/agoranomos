-- The clients of the business, a view over the core (ADR-0024, ADR-0037): the default client, and
-- each client of core.dim_client that an account or a property of the core belongs to. A client
-- that the settings no longer name keeps its row in the core, and its accounts belong to the
-- client that the settings name now, so it leaves the view; so does a client of the demo in a
-- deployment without the demo, as no account of the demo is in the data.
select c.client_id, c.client_name, c.is_default
from {{ ref('dim_client') }} as c
where c.is_default
    or exists (select from {{ ref('dim_account') }} as a where a.client_id = c.client_id)
