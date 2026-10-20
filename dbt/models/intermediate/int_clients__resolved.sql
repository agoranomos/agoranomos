-- The clients of the business (ADR-0024): those of the settings of the deployment and of the
-- demo, the deployment's where both name one ID, and the default client Unassigned.
with ranked as (
    select
        client_id,
        client_name,
        origin,
        row_number() over (
            partition by client_id order by case origin when 'extensions' then 0 else 1 end
        ) as preference
    from {{ ref('stg_settings__clients') }}
)

select client_id, client_name, origin, false as is_default
from ranked
where preference = 1
union all
select 'unassigned', 'Unassigned', 'base', true
