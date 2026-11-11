-- The conversion actions of every platform, a view over the core (ADR-0037): the rows of
-- core.dim_conversion_action, with the category of each, whether the platform counts it in its own
-- metric of conversions, and whether it is primary for the deployment, that is whether it counts
-- in conversions_primary.
select
    k.platform,
    p.platform_name,
    k.conversion_action_id,
    k.conversion_action_name,
    k.conversion_action_status,
    k.platform_category,
    k.conversion_category,
    k.include_in_conversions_metric,
    k.is_primary
from {{ ref('dim_conversion_action') }} as k
left join {{ platforms(ref('seed_ad_platforms')) }}
    on p.platform = k.platform
