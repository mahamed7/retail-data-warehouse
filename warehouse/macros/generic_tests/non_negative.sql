{#
    Fails for every row where the column is below zero. Nulls pass; pair with
    not_null where the column is required.
#}
{% test non_negative(model, column_name) %}

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
