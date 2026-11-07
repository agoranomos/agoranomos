-- core.dim_client, the clients of the business (ADR-0024, ADR-0036): those that the settings of the
-- deployment and the file of the demo name, and the default client Unassigned. The model merges
-- into the table of the migrations (dbt_project.yml), and only the clients that are new or that
-- changed: a client that leaves the settings keeps its row, as its accounts keep their data.
{{ config(unique_key='client_id') }}
with existing as (
    select * from {{ this }}
)

select
    c.client_id::text as client_id,
    c.client_name::text as client_name,
    c.is_default
from {{ ref('int_clients__resolved') }} as c
left join existing as d
    on d.client_id = c.client_id
where d.client_id is null
    or (c.client_name, c.is_default) is distinct from (d.client_name, d.is_default)
