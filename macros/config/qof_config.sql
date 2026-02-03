/*
QOF Configuration

Usage:
  WHERE clinical_effective_date <= {{ qof_reference_date() }}

Edit the date below to change reference date, or use CURRENT_DATE() for live.
*/

{% macro qof_reference_date() %}
    CURRENT_DATE()
{% endmacro %}
