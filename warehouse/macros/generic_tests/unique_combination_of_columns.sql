{#
    Fails for every combination of the given columns that appears more than once.
    Used to assert a model's grain when its key spans several columns.
#}
{% test unique_combination_of_columns(model, combination_of_columns) %}

{%- set column_list = combination_of_columns | join(', ') %}

select
    {{ column_list }},
    count(*) as row_count
from {{ model }}
group by {{ column_list }}
having count(*) > 1

{% endtest %}
