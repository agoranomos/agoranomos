{#- Finds every group of the rows of a dimension of the core, such as the entities of an account,
    in which the stubs, the rows that only the data named and no list gave (ADR-0036), are more
    than the share stub_share_warn of the vars of dbt_project.yml. A stub can be right, so its YAML
    gives this test the severity of a warning: Meta lists no deleted entity, while its statistics
    name them. -#}
{% test few_stubs(model, group_by, stub) %}
select
    {{ group_by }},
    count(*) filter (where {{ stub }}) as stubs,
    count(*) as entities
from {{ model }}
group by {{ group_by }}
having count(*) filter (where {{ stub }}) > {{ var('stub_share_warn') }} * count(*)
{% endtest %}
