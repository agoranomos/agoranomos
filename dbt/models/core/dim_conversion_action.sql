-- core.dim_conversion_action, the registry of conversion actions (ADR-0036, ADR-0038): one row for
-- each conversion action of each platform that the data, the accounts of Google Ads or the seed of
-- conversion actions name, with its surrogate key conversion_action_sk, which the fact of the
-- conversions of campaigns is to carry, and whether it counts as primary, as
-- int_conversion_actions__resolved says. A new action takes the next value of the sequence of the
-- table and keeps it for good, as an account does in core.dim_account. The model merges into the
-- table of the migrations (dbt_project.yml), and only the rows that are new or that changed.
{{ config(
    unique_key=['platform', 'conversion_action_id'],
    merge_exclude_columns=['conversion_action_sk'],
) }}
with existing as (
    select * from {{ this }}
),

keyed as (
    select
        coalesce(
            d.conversion_action_sk,
            nextval('core.dim_conversion_action_conversion_action_sk_seq')
        )::integer as conversion_action_sk,
        r.platform::text as platform,
        r.conversion_action_id::text as conversion_action_id,
        r.conversion_action_name::text as conversion_action_name,
        r.conversion_action_status::text as conversion_action_status,
        r.platform_category::text as platform_category,
        r.conversion_category::text as conversion_category,
        r.include_in_conversions_metric,
        r.is_primary,
        d.conversion_action_sk is null as is_new
    from {{ ref('int_conversion_actions__resolved') }} as r
    left join existing as d
        on d.platform = r.platform and d.conversion_action_id = r.conversion_action_id
)

select
    k.conversion_action_sk,
    k.platform,
    k.conversion_action_id,
    k.conversion_action_name,
    k.conversion_action_status,
    k.platform_category,
    k.conversion_category,
    k.include_in_conversions_metric,
    k.is_primary
from keyed as k
left join existing as d
    on d.platform = k.platform and d.conversion_action_id = k.conversion_action_id
where k.is_new
    or (
        k.conversion_action_name, k.conversion_action_status, k.platform_category,
        k.conversion_category, k.include_in_conversions_metric, k.is_primary
    ) is distinct from (
        d.conversion_action_name, d.conversion_action_status, d.platform_category,
        d.conversion_category, d.include_in_conversions_metric, d.is_primary
    )
