{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid eosinophil count per person.
Filters to valid observations only and returns the most recent per person.
*/

SELECT
    id,
    person_id,
    clinical_effective_date,
    concept_code,
    code_description,
    source_cluster_id,
    original_result_value,
    original_result_unit_display,
    original_result_unit_code,
    expected_measurement_type,
    inferred_unit,
    inferred_value,
    value_was_converted,
    unit_was_changed,
    conversion_reason,
    confidence,
    is_valid_eosinophil,
    eosinophil_category
FROM {{ ref('int_eosinophil_count') }}
WHERE is_valid_eosinophil = TRUE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC
) = 1
