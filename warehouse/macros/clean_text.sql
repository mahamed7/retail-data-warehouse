{#
    Trim a raw text column and turn blank strings into null. The loader already
    writes empty CSV fields as NULL; this guards against whitespace-only values.
#}
{% macro clean_text(column_name) -%}
    nullif(trim({{ column_name }}), '')
{%- endmacro %}
