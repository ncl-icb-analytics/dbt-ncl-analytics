{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All blood eosinophil percentage observations with unit validation and data quality flags.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Uses stricter logic than the count model: only accepted units (%, percent, {WBCs}) are valid.
Standard unit: % (percentage of total WBC count).
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
    FROM ({{ get_observations("'EOS_PERCENT'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.clinical_effective_date <= CURRENT_DATE()
      AND obs.result_value IS NOT NULL

),

unit_rules AS (
    SELECT *
    FROM {{ ref('observation_unit_rules') }}
    WHERE DEFINITION_NAME = 'eosinophil_percentage'
),

unit_checked AS (
    SELECT
        base.*,
        TRY_CAST(base.result_value AS FLOAT) AS numeric_value,
        base.result_unit_code AS original_result_unit_code,
        base.result_unit_display AS original_result_unit_display,
        base.result_value AS original_result_value,
        ur.CONVERT_TO_UNIT AS seed_convert_to_unit,
        ur.MULTIPLY_BY AS seed_multiply_by,
        CASE
            WHEN ur.CONVERT_FROM_UNIT IS NOT NULL AND ur.MULTIPLY_BY IS NULL THEN 'excluded'
            WHEN ur.CONVERT_FROM_UNIT IS NOT NULL AND ur.MULTIPLY_BY IS NOT NULL THEN 'known'
            ELSE 'unknown'
        END AS unit_status,
        (ur.CONVERT_FROM_UNIT = ur.CONVERT_TO_UNIT AND ur.MULTIPLY_BY = 1) AS is_canonical
    FROM base_observations base
    LEFT JOIN unit_rules ur
        ON base.result_unit_code = ur.CONVERT_FROM_UNIT
),

validated AS (
    SELECT
        *,
        CASE
            WHEN unit_status = 'excluded' OR unit_status = 'unknown' THEN NULL
            WHEN numeric_value BETWEEN 0 AND 100 THEN numeric_value
            ELSE NULL
        END AS inferred_value,

        CASE
            WHEN unit_status = 'excluded' OR unit_status = 'unknown' THEN 'NONE'
            WHEN numeric_value BETWEEN 0 AND 100 THEN
                CASE
                    WHEN is_canonical THEN 'VERY_HIGH'
                    ELSE 'HIGH'
                END
            ELSE 'NONE'
        END AS confidence,

        CASE
            WHEN unit_status = 'excluded' OR unit_status = 'unknown' THEN NULL
            WHEN numeric_value BETWEEN 0 AND 100 THEN seed_convert_to_unit
            ELSE NULL
        END AS inferred_unit,

        CASE
            WHEN unit_status = 'excluded' THEN 'Excluded unit on percentage measurement'
            WHEN unit_status = 'unknown' THEN 'Non-accepted unit on percentage measurement'
            WHEN numeric_value BETWEEN 0 AND 100 THEN
                CASE
                    WHEN is_canonical THEN 'Canonical unit, value in range'
                    ELSE 'Accepted unit, value in range'
                END
            ELSE 'Value out of valid percentage range (0-100)'
        END AS conversion_reason,

        CASE
            WHEN unit_status IN ('excluded', 'unknown') THEN FALSE
            ELSE FALSE
        END AS value_was_converted
    FROM unit_checked
),

flagged AS (
    SELECT
        *,
        inferred_value < 0 AS is_negative,
        inferred_value > 80 AS is_extreme_outlier,
        (inferred_value < 0 OR inferred_value > 80 OR confidence = 'NONE') AS is_implausible,
        (NOT (inferred_value < 0 OR inferred_value > 80 OR confidence = 'NONE')
         AND inferred_value IS NOT NULL) AS is_valid_eosinophil
    FROM validated
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
    'PERCENTAGE' AS expected_measurement_type,
    inferred_unit,
    inferred_value,
    value_was_converted,
    inferred_unit IS DISTINCT FROM original_result_unit_code AS unit_was_changed,
    conversion_reason,
    confidence,
    is_negative,
    is_extreme_outlier,
    is_implausible,
    is_valid_eosinophil,
    CASE
        WHEN inferred_value IS NULL OR confidence = 'NONE' THEN 'Invalid'
        WHEN inferred_value <= 5 THEN 'Normal'
        WHEN inferred_value <= 20 THEN 'Elevated'
        WHEN inferred_value <= 80 THEN 'Very Elevated'
        ELSE 'Invalid'
    END AS eosinophil_category
FROM flagged
