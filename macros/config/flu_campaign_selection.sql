/*
Flu Campaign Selection

Usage in models:
  SELECT * FROM ({{ flu_current_config() }})
  SELECT * FROM ({{ flu_previous_config() }})

Override at runtime:
  dbt run --vars '{"flu_current_campaign": "Flu 2024-25"}'
*/

{# ===== Campaign ID getters (return string) ===== #}

{% macro flu_current_campaign() %}
    {{- var('flu_current_campaign', 'Flu 2025-26') -}}
{% endmacro %}

{% macro flu_previous_campaign() %}
    {{- var('flu_previous_campaign', 'Flu 2024-25') -}}
{% endmacro %}

{# ===== Config CTE shortcuts (return full campaign config) ===== #}

{% macro flu_current_config() %}
    {{ flu_campaign_config(flu_current_campaign()) }}
{% endmacro %}

{% macro flu_previous_config() %}
    {{ flu_campaign_config(flu_previous_campaign()) }}
{% endmacro %}
