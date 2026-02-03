/*
COVID Campaign Selection

Usage in models:
  SELECT * FROM ({{ covid_autumn_config() }})
  SELECT * FROM ({{ covid_spring_config() }})
  SELECT * FROM ({{ covid_previous_autumn_config() }})

Override at runtime:
  dbt run --vars '{"covid_current_autumn": "COVID Autumn 2024"}'
*/

{# ===== Campaign ID getters (return string) ===== #}

{% macro covid_current_autumn() %}
    {{- var('covid_current_autumn', 'COVID Autumn 2025') -}}
{% endmacro %}

{% macro covid_current_spring() %}
    {{- var('covid_current_spring', 'COVID Spring 2025') -}}
{% endmacro %}

{% macro covid_previous_autumn() %}
    {{- var('covid_previous_autumn', 'COVID Autumn 2024') -}}
{% endmacro %}

{% macro covid_previous_spring() %}
    {{- var('covid_previous_spring', 'COVID Spring 2025') -}}
{% endmacro %}

{# ===== Config CTE shortcuts (return full campaign config) ===== #}

{% macro covid_autumn_config() %}
    {{ covid_campaign_config(covid_current_autumn()) }}
{% endmacro %}

{% macro covid_spring_config() %}
    {{ covid_campaign_config(covid_current_spring()) }}
{% endmacro %}

{% macro covid_previous_autumn_config() %}
    {{ covid_campaign_config(covid_previous_autumn()) }}
{% endmacro %}

{% macro covid_previous_spring_config() %}
    {{ covid_campaign_config(covid_previous_spring()) }}
{% endmacro %}
