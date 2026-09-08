{{ config(materialized='table', cluster_by=['indicator_id']) }}

/*
Person-level status for every NICE indicator measure in OLIDS_MEASURES, in one
long-form contract. One row per person and indicator on the current reporting
date. Each family union or single-indicator measure keeps its own detail
columns; this model carries only the shared set.

To add a measure, give it the shared columns and append it to the list below.
*/

{% set indicator_models = [
    'fct_person_bp_control_nice_indicators',
    'fct_person_lipid_lowering_therapy_nice_indicators',
    'fct_person_cholesterol_control_ind278'
] %}

{% for indicator_model in indicator_models %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    current_practice_code,
    current_practice_name,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
