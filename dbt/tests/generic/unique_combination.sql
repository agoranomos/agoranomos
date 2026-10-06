{#- Fails for every combination of the columns that appears in more than one row. -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as occurrences
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
