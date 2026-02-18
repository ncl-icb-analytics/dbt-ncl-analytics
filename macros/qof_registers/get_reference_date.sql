/*
DEPRECATED: Use {{ qof_reference_date() }} instead.

This macro is maintained for backward compatibility.
*/

{% macro get_reference_date() %}
    {{ qof_reference_date() }}
{% endmacro %}
