{% macro to_sql_list(values) %}
    {% if values is none or values | length == 0 %}
        (NULL)
    {% else %}
        (
        {%- for v in values -%}
            '{{ v }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
        )
    {% endif %}
{% endmacro %}
