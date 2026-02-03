/*
COVID Campaign Configuration

Usage:
  WHERE observation_date >= {{ covid_autumn_campaign_start_date() }}
    AND observation_date <= {{ covid_autumn_campaign_end_date() }}

To change campaign year, update the defaults in covid_current_autumn() etc below.
*/

{# ===== Campaign definitions (single source of truth) ===== #}

{% macro _covid_campaigns() %}
    {% set campaigns = {
        'COVID Autumn 2025': {
            'campaign_name': 'Autumn 2025 COVID Vaccination Campaign',
            'campaign_year': 'covid_2025_26',
            'campaign_period': 'autumn',
            'campaign_start_date': '2025-09-01',
            'campaign_end_date': '2026-03-31',
            'campaign_reference_date': '2026-03-31',
            'immuno_medication_lookback_date': '2025-03-01',
            'asthma_medication_lookback_date': '2024-09-01',
            'asthma_admission_lookback_date': '2023-09-01',
            'vaccination_tracking_start': '2025-09-01',
            'vaccination_tracking_end': '2026-03-31',
            'decline_tracking_start': '2025-08-01',
            'decline_tracking_end': '2026-06-30'
        },
        'COVID Autumn 2024': {
            'campaign_name': 'Autumn 2024 COVID Vaccination Campaign',
            'campaign_year': 'covid_2024_25',
            'campaign_period': 'autumn',
            'campaign_start_date': '2024-09-01',
            'campaign_end_date': '2025-03-31',
            'campaign_reference_date': '2025-03-31',
            'immuno_medication_lookback_date': '2024-03-01',
            'asthma_medication_lookback_date': '2022-09-01',
            'asthma_admission_lookback_date': '2022-09-01',
            'vaccination_tracking_start': '2024-09-01',
            'vaccination_tracking_end': '2025-03-31',
            'decline_tracking_start': '2024-08-01',
            'decline_tracking_end': '2025-06-30'
        },
        'COVID Spring 2025': {
            'campaign_name': 'Spring 2025 COVID Vaccination Campaign',
            'campaign_year': 'covid_2024_25',
            'campaign_period': 'spring',
            'campaign_start_date': '2025-04-01',
            'campaign_end_date': '2025-06-30',
            'campaign_reference_date': '2025-06-30',
            'immuno_medication_lookback_date': '2024-10-01',
            'asthma_medication_lookback_date': '2024-04-01',
            'asthma_admission_lookback_date': '2023-04-01',
            'vaccination_tracking_start': '2025-04-01',
            'vaccination_tracking_end': '2025-06-30',
            'decline_tracking_start': '2025-03-01',
            'decline_tracking_end': '2025-06-30'
        }
    } %}
    {{ return(campaigns) }}
{% endmacro %}

{# ===== Campaign ID selectors (edit these to change campaign year) ===== #}

{% macro covid_current_autumn() %}
    {{- 'COVID Autumn 2025' -}}
{% endmacro %}

{% macro covid_current_spring() %}
    {{- 'COVID Spring 2025' -}}
{% endmacro %}

{% macro covid_previous_autumn() %}
    {{- 'COVID Autumn 2024' -}}
{% endmacro %}

{% macro covid_previous_spring() %}
    {{- 'COVID Spring 2025' -}}
{% endmacro %}

{# ===== Current autumn campaign field accessors ===== #}

{% macro covid_autumn_campaign_name() %}
    '{{ _covid_campaigns()[covid_current_autumn()]['campaign_name'] }}'
{% endmacro %}

{% macro covid_autumn_campaign_start_date() %}
    '{{ _covid_campaigns()[covid_current_autumn()]['campaign_start_date'] }}'::DATE
{% endmacro %}

{% macro covid_autumn_campaign_end_date() %}
    '{{ _covid_campaigns()[covid_current_autumn()]['campaign_end_date'] }}'::DATE
{% endmacro %}

{% macro covid_autumn_campaign_reference_date() %}
    '{{ _covid_campaigns()[covid_current_autumn()]['campaign_reference_date'] }}'::DATE
{% endmacro %}

{% macro covid_autumn_vaccination_tracking_start() %}
    '{{ _covid_campaigns()[covid_current_autumn()]['vaccination_tracking_start'] }}'::DATE
{% endmacro %}

{% macro covid_autumn_vaccination_tracking_end() %}
    '{{ _covid_campaigns()[covid_current_autumn()]['vaccination_tracking_end'] }}'::DATE
{% endmacro %}

{# ===== Current spring campaign field accessors ===== #}

{% macro covid_spring_campaign_name() %}
    '{{ _covid_campaigns()[covid_current_spring()]['campaign_name'] }}'
{% endmacro %}

{% macro covid_spring_campaign_start_date() %}
    '{{ _covid_campaigns()[covid_current_spring()]['campaign_start_date'] }}'::DATE
{% endmacro %}

{% macro covid_spring_campaign_end_date() %}
    '{{ _covid_campaigns()[covid_current_spring()]['campaign_end_date'] }}'::DATE
{% endmacro %}

{% macro covid_spring_campaign_reference_date() %}
    '{{ _covid_campaigns()[covid_current_spring()]['campaign_reference_date'] }}'::DATE
{% endmacro %}

{# ===== Previous autumn campaign field accessors ===== #}

{% macro covid_previous_autumn_campaign_name() %}
    '{{ _covid_campaigns()[covid_previous_autumn()]['campaign_name'] }}'
{% endmacro %}

{% macro covid_previous_autumn_campaign_start_date() %}
    '{{ _covid_campaigns()[covid_previous_autumn()]['campaign_start_date'] }}'::DATE
{% endmacro %}

{% macro covid_previous_autumn_campaign_end_date() %}
    '{{ _covid_campaigns()[covid_previous_autumn()]['campaign_end_date'] }}'::DATE
{% endmacro %}

{% macro covid_previous_autumn_campaign_reference_date() %}
    '{{ _covid_campaigns()[covid_previous_autumn()]['campaign_reference_date'] }}'::DATE
{% endmacro %}

{# ===== Legacy CTE-style config (for backward compatibility) ===== #}

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
