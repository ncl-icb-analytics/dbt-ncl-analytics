/*
DEPRECATED: Use macros/config/qof_config.sql instead.

This macro is maintained for backward compatibility but simply delegates
to get_qof_reference_date() in the config module.

Migration:
  Old: {{ get_reference_date() }}
  New: {{ get_qof_reference_date() }}
*/

{% macro get_reference_date() %}
    {{ get_qof_reference_date() }}
{% endmacro %}
