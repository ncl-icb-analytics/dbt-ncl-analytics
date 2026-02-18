/*
QOF Configuration

Usage:
  WHERE clinical_effective_date <= {{ qof_reference_date() }}

Override at runtime:
  dbt run --vars '{"qof_reference_date": "2025-03-31"}'
*/

{% macro qof_reference_date() %}
    {%- if var('qof_reference_date', none) -%}
        '{{ var('qof_reference_date') }}'::DATE
    {%- else -%}
        CURRENT_DATE()
    {%- endif -%}
{% endmacro %}
