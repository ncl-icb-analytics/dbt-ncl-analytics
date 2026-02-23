/*
QOF Configuration

Usage:
  WHERE clinical_effective_date <= {{ qof_reference_date() }}

Override at runtime:
  dbt run --vars '{"qof_reference_date": "2025-03-31"}'
*/

{% macro qof_reference_date() %}
    {%- set ref_date = var('qof_reference_date', '2025-11-04') -%}
    {%- if ref_date | upper in ('CURRENT_DATE', 'CURRENT_DATE()') -%}
        CURRENT_DATE()
    {%- else -%}
        '{{ ref_date }}'::DATE
    {%- endif -%}
{% endmacro %}
