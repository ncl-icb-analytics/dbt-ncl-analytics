{% macro get_reference_date() %}
    {#
    Returns reference date for QOF register calculations.
    Override via dbt var: --vars '{"qof_reference_date": "2024-03-31"}'
    #}
    {% if var('qof_reference_date', none) %}
        '{{ var('qof_reference_date') }}'::DATE
    {% else %}
        CURRENT_DATE()
    {% endif %}
{% endmacro %}
