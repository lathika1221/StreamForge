{#- Fails for every row where the boolean expression is not true (false or null).
    Local replacement for dbt_utils (no package download needed). -#}
{% test expression_is_true(model, expression) %}
select *
from {{ model }}
where not coalesce(({{ expression }}), false)
{% endtest %}
