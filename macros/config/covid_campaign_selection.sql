/*
COVID Campaign Selection Configuration

Centralizes campaign selection logic in one place.
Change these values when switching to a new campaign year.

COVID has separate autumn and spring campaigns, plus previous versions for comparison.

Previously these were scattered across dbt_project.yml vars.
Now they live here for better organization and documentation.

Usage in models:
  {{ covid_campaign_config(get_covid_current_autumn()) }}
  {{ covid_campaign_config(get_covid_current_spring()) }}
*/

{% macro get_covid_current_autumn() %}
    {#- Returns the current autumn COVID campaign identifier -#}
    {#- Override via dbt var: --vars '{"covid_current_autumn": "COVID Autumn 2024"}' -#}
    {{ return(var('covid_current_autumn', 'COVID Autumn 2025')) }}
{% endmacro %}

{% macro get_covid_current_spring() %}
    {#- Returns the current spring COVID campaign identifier -#}
    {#- Override via dbt var: --vars '{"covid_current_spring": "COVID Spring 2024"}' -#}
    {{ return(var('covid_current_spring', 'COVID Spring 2025')) }}
{% endmacro %}

{% macro get_covid_previous_autumn() %}
    {#- Returns the previous autumn COVID campaign identifier (for YoY comparison) -#}
    {#- Override via dbt var: --vars '{"covid_previous_autumn": "COVID Autumn 2023"}' -#}
    {{ return(var('covid_previous_autumn', 'COVID Autumn 2024')) }}
{% endmacro %}

{% macro get_covid_previous_spring() %}
    {#- Returns the previous spring COVID campaign identifier (for comparison) -#}
    {#- Override via dbt var: --vars '{"covid_previous_spring": "COVID Spring 2024"}' -#}
    {{ return(var('covid_previous_spring', 'COVID Spring 2025')) }}
{% endmacro %}

{% macro get_covid_audit_end_date() %}
    {#- Returns the audit end date for COVID campaign reporting -#}
    {#- Override via dbt var: --vars '{"covid_audit_end_date": "2025-06-30"}' -#}
    {{ return(var('covid_audit_end_date', '2025-06-30')) }}
{% endmacro %}
