{#- Fails for every version of a snapshot of the core that does not end where the next version of
    its entity begins, as when two versions overlap, and for the newest version of an entity that
    does not hold to current_to, the end of a version that holds, dbt_valid_to_current of
    dbt_project.yml (ADR-0036): with hard_deletes ignore, the newest version of an entity holds,
    even when the newest list no longer names it. -#}
{% test versions_follow_each_other(model, key_columns, current_to) %}
with ordered as (
    select
        {{ key_columns | join(', ') }},
        dbt_valid_from,
        dbt_valid_to,
        lead(dbt_valid_from) over (
            partition by {{ key_columns | join(', ') }} order by dbt_valid_from, dbt_valid_to
        ) as next_from
    from {{ model }}
)
select *
from ordered
where dbt_valid_from >= dbt_valid_to
    or dbt_valid_to is distinct from coalesce(next_from, {{ current_to }})
{% endtest %}
