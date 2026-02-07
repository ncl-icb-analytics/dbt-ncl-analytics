/*
COVID Campaign Configuration

Usage:
  WHERE observation_date >= {{ covid_autumn_campaign_start_date() }}
    AND observation_date <= {{ covid_autumn_campaign_end_date() }}

To change campaign year, update covid_current_autumn() etc below.
*/

{# ===== Campaign ID selectors (edit these to change campaign year) ===== #}

{% macro covid_current_autumn() %}COVID Autumn 2025{% endmacro %}
{% macro covid_current_spring() %}COVID Spring 2025{% endmacro %}
{% macro covid_previous_autumn() %}COVID Autumn 2024{% endmacro %}
{% macro covid_previous_spring() %}COVID Spring 2024{% endmacro %}

{# ===== Campaign data lookup ===== #}

{% macro _covid_campaign_data(campaign_id, field) %}
    {%- if campaign_id == 'COVID Autumn 2025' -%}
        {%- if field == 'campaign_name' -%}Autumn 2025 COVID Vaccination Campaign
        {%- elif field == 'campaign_start_date' -%}'2025-09-01'::DATE
        {%- elif field == 'campaign_end_date' -%}'2026-03-31'::DATE
        {%- elif field == 'campaign_reference_date' -%}'2026-03-31'::DATE
        {%- elif field == 'vaccination_tracking_start' -%}'2025-09-01'::DATE
        {%- elif field == 'vaccination_tracking_end' -%}'2026-03-31'::DATE
        {%- elif field == 'decline_tracking_start' -%}'2025-08-01'::DATE
        {%- elif field == 'decline_tracking_end' -%}'2026-06-30'::DATE
        {%- else -%}{{ exceptions.raise_compiler_error("Unknown field '" ~ field ~ "' for campaign '" ~ campaign_id ~ "'") }}
        {%- endif -%}
    {%- elif campaign_id == 'COVID Autumn 2024' -%}
        {%- if field == 'campaign_name' -%}Autumn 2024 COVID Vaccination Campaign
        {%- elif field == 'campaign_start_date' -%}'2024-09-01'::DATE
        {%- elif field == 'campaign_end_date' -%}'2025-03-31'::DATE
        {%- elif field == 'campaign_reference_date' -%}'2025-03-31'::DATE
        {%- elif field == 'vaccination_tracking_start' -%}'2024-09-01'::DATE
        {%- elif field == 'vaccination_tracking_end' -%}'2025-03-31'::DATE
        {%- elif field == 'decline_tracking_start' -%}'2024-08-01'::DATE
        {%- elif field == 'decline_tracking_end' -%}'2025-06-30'::DATE
        {%- else -%}{{ exceptions.raise_compiler_error("Unknown field '" ~ field ~ "' for campaign '" ~ campaign_id ~ "'") }}
        {%- endif -%}
    {%- elif campaign_id == 'COVID Spring 2025' -%}
        {%- if field == 'campaign_name' -%}Spring 2025 COVID Vaccination Campaign
        {%- elif field == 'campaign_start_date' -%}'2025-04-01'::DATE
        {%- elif field == 'campaign_end_date' -%}'2025-06-30'::DATE
        {%- elif field == 'campaign_reference_date' -%}'2025-06-30'::DATE
        {%- elif field == 'vaccination_tracking_start' -%}'2025-04-01'::DATE
        {%- elif field == 'vaccination_tracking_end' -%}'2025-06-30'::DATE
        {%- elif field == 'decline_tracking_start' -%}'2025-03-01'::DATE
        {%- elif field == 'decline_tracking_end' -%}'2025-06-30'::DATE
        {%- else -%}{{ exceptions.raise_compiler_error("Unknown field '" ~ field ~ "' for campaign '" ~ campaign_id ~ "'") }}
        {%- endif -%}
    {%- elif campaign_id == 'COVID Spring 2024' -%}
        {%- if field == 'campaign_name' -%}Spring 2024 COVID Vaccination Campaign
        {%- elif field == 'campaign_start_date' -%}'2024-04-01'::DATE
        {%- elif field == 'campaign_end_date' -%}'2024-06-30'::DATE
        {%- elif field == 'campaign_reference_date' -%}'2024-06-30'::DATE
        {%- elif field == 'vaccination_tracking_start' -%}'2024-04-01'::DATE
        {%- elif field == 'vaccination_tracking_end' -%}'2024-06-30'::DATE
        {%- elif field == 'decline_tracking_start' -%}'2024-03-01'::DATE
        {%- elif field == 'decline_tracking_end' -%}'2024-06-30'::DATE
        {%- else -%}{{ exceptions.raise_compiler_error("Unknown field '" ~ field ~ "' for campaign '" ~ campaign_id ~ "'") }}
        {%- endif -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error("Unknown COVID campaign_id: '" ~ campaign_id ~ "'. Valid campaigns: COVID Autumn 2025, COVID Autumn 2024, COVID Spring 2025, COVID Spring 2024") }}
    {%- endif -%}
{% endmacro %}

{# ===== Current autumn campaign field accessors ===== #}

{% macro covid_autumn_campaign_name() %}{{ _covid_campaign_data(covid_current_autumn(), 'campaign_name') }}{% endmacro %}
{% macro covid_autumn_campaign_start_date() %}{{ _covid_campaign_data(covid_current_autumn(), 'campaign_start_date') }}{% endmacro %}
{% macro covid_autumn_campaign_end_date() %}{{ _covid_campaign_data(covid_current_autumn(), 'campaign_end_date') }}{% endmacro %}
{% macro covid_autumn_campaign_reference_date() %}{{ _covid_campaign_data(covid_current_autumn(), 'campaign_reference_date') }}{% endmacro %}
{% macro covid_autumn_vaccination_tracking_start() %}{{ _covid_campaign_data(covid_current_autumn(), 'vaccination_tracking_start') }}{% endmacro %}
{% macro covid_autumn_vaccination_tracking_end() %}{{ _covid_campaign_data(covid_current_autumn(), 'vaccination_tracking_end') }}{% endmacro %}

{# ===== Current spring campaign field accessors ===== #}

{% macro covid_spring_campaign_name() %}{{ _covid_campaign_data(covid_current_spring(), 'campaign_name') }}{% endmacro %}
{% macro covid_spring_campaign_start_date() %}{{ _covid_campaign_data(covid_current_spring(), 'campaign_start_date') }}{% endmacro %}
{% macro covid_spring_campaign_end_date() %}{{ _covid_campaign_data(covid_current_spring(), 'campaign_end_date') }}{% endmacro %}
{% macro covid_spring_campaign_reference_date() %}{{ _covid_campaign_data(covid_current_spring(), 'campaign_reference_date') }}{% endmacro %}

{# ===== Previous autumn campaign field accessors ===== #}

{% macro covid_previous_autumn_campaign_name() %}{{ _covid_campaign_data(covid_previous_autumn(), 'campaign_name') }}{% endmacro %}
{% macro covid_previous_autumn_campaign_start_date() %}{{ _covid_campaign_data(covid_previous_autumn(), 'campaign_start_date') }}{% endmacro %}
{% macro covid_previous_autumn_campaign_end_date() %}{{ _covid_campaign_data(covid_previous_autumn(), 'campaign_end_date') }}{% endmacro %}
{% macro covid_previous_autumn_campaign_reference_date() %}{{ _covid_campaign_data(covid_previous_autumn(), 'campaign_reference_date') }}{% endmacro %}

{# ===== Legacy CTE-style config (for backward compatibility) ===== #}

{% macro covid_autumn_config() %}{{ covid_campaign_config(covid_current_autumn()) }}{% endmacro %}
{% macro covid_spring_config() %}{{ covid_campaign_config(covid_current_spring()) }}{% endmacro %}
{% macro covid_previous_autumn_config() %}{{ covid_campaign_config(covid_previous_autumn()) }}{% endmacro %}
{% macro covid_previous_spring_config() %}{{ covid_campaign_config(covid_previous_spring()) }}{% endmacro %}
