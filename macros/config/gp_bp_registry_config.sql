/*
GP BP Registry (QMUL) configuration

Tuneables for the research cohort build. All macros emit integer literals so
they can be used directly inside DATEADD / DATEDIFF / comparison expressions.

Usage:
  DATEADD('week', {{ gp_bp_registry_hdp_postpartum_weeks() }}, some_date)

Override at runtime:
  dbt run --vars '{"gp_bp_registry_hdp_postpartum_weeks": 12}'
*/

{% macro gp_bp_registry_max_pregnancy_episode_weeks() %}
    {{ var('gp_bp_registry_max_pregnancy_episode_weeks', 42) }}
{% endmacro %}

{% macro gp_bp_registry_hdp_postpartum_weeks() %}
    {{ var('gp_bp_registry_hdp_postpartum_weeks', 8) }}
{% endmacro %}

{% macro gp_bp_registry_min_reading_gap_weeks() %}
    {{ var('gp_bp_registry_min_reading_gap_weeks', 4) }}
{% endmacro %}

{% macro gp_bp_registry_min_qualifying_readings() %}
    {{ var('gp_bp_registry_min_qualifying_readings', 4) }}
{% endmacro %}

{% macro gp_bp_registry_min_span_months() %}
    {{ var('gp_bp_registry_min_span_months', 36) }}
{% endmacro %}
