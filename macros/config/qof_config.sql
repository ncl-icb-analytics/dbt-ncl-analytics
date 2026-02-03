/*
QOF Configuration

Centralizes QOF-related configuration in one place.
The reference date is used for point-in-time register calculations.

Previously this was in qof_registers/get_reference_date.sql and dbt_project.yml.
Now consolidated here for better organization.

Usage in models:
  WHERE clinical_effective_date <= {{ get_qof_reference_date() }}
*/

{% macro get_qof_reference_date() %}
    {#-
    Returns reference date for QOF register calculations.
    
    Set to match EMIS extract date for validation, or use CURRENT_DATE for live calculations.
    
    Override via dbt var: --vars '{"qof_reference_date": "2024-03-31"}'
    -#}
    {% if var('qof_reference_date', none) %}
        '{{ var('qof_reference_date') }}'::DATE
    {% else %}
        CURRENT_DATE()
    {% endif %}
{% endmacro %}

{% macro get_qof_default_reference_date() %}
    {#- Returns the default reference date (useful for documentation) -#}
    {{ return('2025-11-04') }}
{% endmacro %}
