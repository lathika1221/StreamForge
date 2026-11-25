{#- Fails for every combination of the given columns that occurs more than once.
    Local replacement for dbt_utils (no package download needed). -#}
{% test unique_combination_of_columns(model, combination_of_columns) %}
select
    {{ combination_of_columns | join(', ') }},
    count(*) as occurrences
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1
{% endtest %}
