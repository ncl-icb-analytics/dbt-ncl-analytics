/*
Flu Campaign Selection Configuration

Centralizes campaign selection logic in one place.
Change these values when switching to a new campaign year.

Previously these were scattered across dbt_project.yml vars.
Now they live here for better organization and documentation.

Usage in models:
  {{ flu_campaign_config(get_flu_current_campaign()) }}
  {{ flu_campaign_config(get_flu_previous_campaign()) }}
*/

{% macro get_flu_current_campaign() %}
    {#- Returns the current flu campaign identifier -#}
    {#- Override via dbt var: --vars '{"flu_current_campaign": "Flu 2024-25"}' -#}
    {{ return(var('flu_current_campaign', 'Flu 2025-26')) }}
{% endmacro %}

{% macro get_flu_previous_campaign() %}
    {#- Returns the previous flu campaign identifier (for YoY comparison) -#}
    {#- Override via dbt var: --vars '{"flu_previous_campaign": "Flu 2023-24"}' -#}
    {{ return(var('flu_previous_campaign', 'Flu 2024-25')) }}
{% endmacro %}

{% macro get_flu_audit_end_date() %}
    {#- Returns the audit end date for flu campaign reporting -#}
    {#- Override via dbt var: --vars '{"flu_audit_end_date": "2025-03-31"}' -#}
    {{ return(var('flu_audit_end_date', 'CURRENT_DATE')) }}
{% endmacro %}
