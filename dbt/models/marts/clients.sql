-- The clients of the business (ADR-0024): those of the settings of the deployment, those of the
-- demo while an account or a property of the demo is in the data, and the default client.
select c.client_id, c.client_name, c.is_default
from {{ ref('int_clients__resolved') }} as c
where c.origin <> 'demo'
    or exists (select 1 from {{ ref('int_ad_accounts__unioned') }} as a where a.client_id = c.client_id)
    or exists (select 1 from {{ ref('int_web_sessions__unioned') }} as w where w.client_id = c.client_id)
