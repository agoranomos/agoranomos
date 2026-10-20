-- An ad account that no setting names belongs to the default client: a warning, as a deployment
-- may not have named its clients yet (ADR-0024).
{{ config(severity='warn') }}
select platform, account_id
from {{ ref('int_ad_accounts__unioned') }}
where client_id = 'unassigned'
