{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All blood eosinophil count observations with unit standardisation and data quality flags.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Uses the standardise_count_observation macro for value-first unit inference.
Standard unit: 10*9/L (billion per liter).
*/

WITH base_observations AS (

    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.result_value,
        obs.result_unit_code,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS code_description,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'EOS_COUNT'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.clinical_effective_date <= CURRENT_DATE()
      AND obs.result_value IS NOT NULL

),

{{ standardise_count_observation(
    base_cte='base_observations',
    measurement='eosinophil_count',
    value_column='result_value',
    max_plausible_value=100,
    enable_magnitude_conversion=true
) }}

,

validated AS (
    SELECT
        *,
        inferred_value < 0 AS is_negative,
        inferred_value > 100 AS is_extreme_outlier,
        (NOT (inferred_value < 0 OR inferred_value > 100 OR confidence = 'NONE')
         AND inferred_value IS NOT NULL) AS is_valid_eosinophil
    FROM standardised
)

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
    'COUNT' AS expected_measurement_type,
    inferred_unit,
    inferred_value,
    value_was_converted,
    inferred_unit IS DISTINCT FROM original_result_unit_code AS unit_was_changed,
    conversion_reason,
    confidence,
    is_negative,
    is_extreme_outlier,
    is_valid_eosinophil,
    CASE
        WHEN inferred_value IS NULL OR confidence = 'NONE' THEN 'Invalid'
        WHEN inferred_value < 0.04 THEN 'Eosinopenia'
        WHEN inferred_value <= 0.5 THEN 'Normal'
        WHEN inferred_value <= 1.5 THEN 'Mild Eosinophilia'
        WHEN inferred_value <= 5.0 THEN 'Moderate Eosinophilia'
        WHEN inferred_value <= 100 THEN 'Severe Eosinophilia'
        ELSE 'Invalid'
    END AS eosinophil_category
FROM validated
