/*
Flu Campaign Configuration

Usage:
  WHERE observation_date >= {{ flu_current_campaign_start_date() }}
    AND observation_date <= {{ flu_current_campaign_end_date() }}

To change campaign year, update flu_current_campaign() and flu_previous_campaign() below.
*/

{# ===== Campaign ID selectors (edit these to change campaign year) ===== #}

{% macro flu_current_campaign() %}Flu 2025-26{% endmacro %}
{% macro flu_previous_campaign() %}Flu 2024-25{% endmacro %}

{# ===== Campaign data lookup ===== #}

{% macro _flu_campaign_data(campaign_id, field) %}
    {%- if campaign_id == 'Flu 2025-26' -%}
        {%- if field == 'campaign_name' -%}2025-26 Flu Vaccination Campaign
        {%- elif field == 'campaign_start_date' -%}'2025-09-01'::DATE
        {%- elif field == 'campaign_end_date' -%}'2026-02-28'::DATE
        {%- elif field == 'campaign_reference_date' -%}'2026-03-31'::DATE
        {%- elif field == 'child_reference_date' -%}'2025-08-31'::DATE
        {%- elif field == 'asthma_medication_lookback_date' -%}'2024-09-01'::DATE
        {%- elif field == 'immuno_medication_lookback_date' -%}'2025-03-01'::DATE
        {%- elif field == 'child_preschool_birth_start' -%}'2021-09-01'::DATE
        {%- elif field == 'child_preschool_birth_end' -%}'2023-08-31'::DATE
        {%- elif field == 'child_school_age_birth_start' -%}'2009-09-01'::DATE
        {%- elif field == 'child_school_age_birth_end' -%}'2021-08-31'::DATE
        {%- elif field == 'flu_vaccination_after_date' -%}'2025-08-31'::DATE
        {%- elif field == 'laiv_vaccination_after_date' -%}'2025-08-31'::DATE
        {%- endif -%}
    {%- elif campaign_id == 'Flu 2024-25' -%}
        {%- if field == 'campaign_name' -%}2024-25 Flu Vaccination Campaign
        {%- elif field == 'campaign_start_date' -%}'2024-09-01'::DATE
        {%- elif field == 'campaign_end_date' -%}'2025-02-28'::DATE
        {%- elif field == 'campaign_reference_date' -%}'2025-03-31'::DATE
        {%- elif field == 'child_reference_date' -%}'2024-08-31'::DATE
        {%- elif field == 'asthma_medication_lookback_date' -%}'2023-09-01'::DATE
        {%- elif field == 'immuno_medication_lookback_date' -%}'2024-03-01'::DATE
        {%- elif field == 'child_preschool_birth_start' -%}'2020-09-01'::DATE
        {%- elif field == 'child_preschool_birth_end' -%}'2022-08-31'::DATE
        {%- elif field == 'child_school_age_birth_start' -%}'2008-09-01'::DATE
        {%- elif field == 'child_school_age_birth_end' -%}'2020-08-31'::DATE
        {%- elif field == 'flu_vaccination_after_date' -%}'2024-08-31'::DATE
        {%- elif field == 'laiv_vaccination_after_date' -%}'2024-08-31'::DATE
        {%- endif -%}
    {%- elif campaign_id == 'Flu 2023-24' -%}
        {%- if field == 'campaign_name' -%}2023-24 Flu Vaccination Campaign
        {%- elif field == 'campaign_start_date' -%}'2023-09-01'::DATE
        {%- elif field == 'campaign_end_date' -%}'2024-03-31'::DATE
        {%- elif field == 'campaign_reference_date' -%}'2024-03-31'::DATE
        {%- elif field == 'child_reference_date' -%}'2023-08-31'::DATE
        {%- elif field == 'asthma_medication_lookback_date' -%}'2022-09-01'::DATE
        {%- elif field == 'immuno_medication_lookback_date' -%}'2023-03-01'::DATE
        {%- elif field == 'child_preschool_birth_start' -%}'2019-09-01'::DATE
        {%- elif field == 'child_preschool_birth_end' -%}'2021-08-31'::DATE
        {%- elif field == 'child_school_age_birth_start' -%}'2007-09-01'::DATE
        {%- elif field == 'child_school_age_birth_end' -%}'2019-08-31'::DATE
        {%- elif field == 'flu_vaccination_after_date' -%}'2023-08-31'::DATE
        {%- elif field == 'laiv_vaccination_after_date' -%}'2023-08-31'::DATE
        {%- endif -%}
    {%- endif -%}
{% endmacro %}

{# ===== Current campaign field accessors ===== #}

{% macro flu_current_campaign_name() %}{{ _flu_campaign_data(flu_current_campaign(), 'campaign_name') }}{% endmacro %}
{% macro flu_current_campaign_start_date() %}{{ _flu_campaign_data(flu_current_campaign(), 'campaign_start_date') }}{% endmacro %}
{% macro flu_current_campaign_end_date() %}{{ _flu_campaign_data(flu_current_campaign(), 'campaign_end_date') }}{% endmacro %}
{% macro flu_current_campaign_reference_date() %}{{ _flu_campaign_data(flu_current_campaign(), 'campaign_reference_date') }}{% endmacro %}
{% macro flu_current_child_reference_date() %}{{ _flu_campaign_data(flu_current_campaign(), 'child_reference_date') }}{% endmacro %}
{% macro flu_current_asthma_medication_lookback_date() %}{{ _flu_campaign_data(flu_current_campaign(), 'asthma_medication_lookback_date') }}{% endmacro %}
{% macro flu_current_immuno_medication_lookback_date() %}{{ _flu_campaign_data(flu_current_campaign(), 'immuno_medication_lookback_date') }}{% endmacro %}
{% macro flu_current_child_preschool_birth_start() %}{{ _flu_campaign_data(flu_current_campaign(), 'child_preschool_birth_start') }}{% endmacro %}
{% macro flu_current_child_preschool_birth_end() %}{{ _flu_campaign_data(flu_current_campaign(), 'child_preschool_birth_end') }}{% endmacro %}
{% macro flu_current_child_school_age_birth_start() %}{{ _flu_campaign_data(flu_current_campaign(), 'child_school_age_birth_start') }}{% endmacro %}
{% macro flu_current_child_school_age_birth_end() %}{{ _flu_campaign_data(flu_current_campaign(), 'child_school_age_birth_end') }}{% endmacro %}
{% macro flu_current_vaccination_after_date() %}{{ _flu_campaign_data(flu_current_campaign(), 'flu_vaccination_after_date') }}{% endmacro %}
{% macro flu_current_laiv_vaccination_after_date() %}{{ _flu_campaign_data(flu_current_campaign(), 'laiv_vaccination_after_date') }}{% endmacro %}

{# ===== Previous campaign field accessors ===== #}

{% macro flu_previous_campaign_name() %}{{ _flu_campaign_data(flu_previous_campaign(), 'campaign_name') }}{% endmacro %}
{% macro flu_previous_campaign_start_date() %}{{ _flu_campaign_data(flu_previous_campaign(), 'campaign_start_date') }}{% endmacro %}
{% macro flu_previous_campaign_end_date() %}{{ _flu_campaign_data(flu_previous_campaign(), 'campaign_end_date') }}{% endmacro %}
{% macro flu_previous_campaign_reference_date() %}{{ _flu_campaign_data(flu_previous_campaign(), 'campaign_reference_date') }}{% endmacro %}
{% macro flu_previous_child_reference_date() %}{{ _flu_campaign_data(flu_previous_campaign(), 'child_reference_date') }}{% endmacro %}
{% macro flu_previous_asthma_medication_lookback_date() %}{{ _flu_campaign_data(flu_previous_campaign(), 'asthma_medication_lookback_date') }}{% endmacro %}
{% macro flu_previous_immuno_medication_lookback_date() %}{{ _flu_campaign_data(flu_previous_campaign(), 'immuno_medication_lookback_date') }}{% endmacro %}
{% macro flu_previous_child_preschool_birth_start() %}{{ _flu_campaign_data(flu_previous_campaign(), 'child_preschool_birth_start') }}{% endmacro %}
{% macro flu_previous_child_preschool_birth_end() %}{{ _flu_campaign_data(flu_previous_campaign(), 'child_preschool_birth_end') }}{% endmacro %}
{% macro flu_previous_child_school_age_birth_start() %}{{ _flu_campaign_data(flu_previous_campaign(), 'child_school_age_birth_start') }}{% endmacro %}
{% macro flu_previous_child_school_age_birth_end() %}{{ _flu_campaign_data(flu_previous_campaign(), 'child_school_age_birth_end') }}{% endmacro %}
{% macro flu_previous_vaccination_after_date() %}{{ _flu_campaign_data(flu_previous_campaign(), 'flu_vaccination_after_date') }}{% endmacro %}
{% macro flu_previous_laiv_vaccination_after_date() %}{{ _flu_campaign_data(flu_previous_campaign(), 'laiv_vaccination_after_date') }}{% endmacro %}

{# ===== Legacy CTE-style config (for backward compatibility) ===== #}

{% macro flu_current_config() %}{{ flu_campaign_config(flu_current_campaign()) }}{% endmacro %}
{% macro flu_previous_config() %}{{ flu_campaign_config(flu_previous_campaign()) }}{% endmacro %}
