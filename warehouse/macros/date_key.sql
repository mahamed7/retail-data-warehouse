{# Integer YYYYMMDD key for a date or timestamp expression, shared by dim_date and fct_sales. #}
{% macro date_key(date_expression) -%}
    to_char({{ date_expression }}, 'YYYYMMDD')::integer
{%- endmacro %}
