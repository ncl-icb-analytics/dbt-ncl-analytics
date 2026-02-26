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

canonical_unit AS (
    SELECT UNIT AS CONVERT_TO_UNIT
    FROM {{ ref('observation_standard_units') }}
    WHERE DEFINITION_NAME = 'eosinophil_percentage'
      AND PRIMARY_UNIT = TRUE
),

value_bounds AS (
    SELECT LOWER_LIMIT, UPPER_LIMIT
    FROM {{ ref('observation_value_bounds') }}
    WHERE DEFINITION_NAME = 'eosinophil_percentage'
),

unit_checked AS (
    SELECT
        base.*,
        TRY_CAST(base.result_value AS FLOAT) AS numeric_value,
        base.result_unit_code AS original_result_unit_code,
        base.result_unit_display AS original_result_unit_display,
        base.result_value AS original_result_value,
        cu.CONVERT_TO_UNIT AS canonical_unit_code,
        vb.LOWER_LIMIT,
        vb.UPPER_LIMIT,
        CASE
            WHEN ur.CONVERT_FROM_UNIT IS NOT NULL AND ur.MULTIPLY_BY IS NULL THEN 'excluded'
            WHEN ur.CONVERT_FROM_UNIT IS NOT NULL AND ur.MULTIPLY_BY IS NOT NULL THEN 'known'
            ELSE 'unknown'
        END AS unit_status,
        (ur.CONVERT_FROM_UNIT = ur.CONVERT_TO_UNIT AND ur.MULTIPLY_BY = 1) AS is_canonical
    FROM base_observations base
    LEFT JOIN unit_rules ur
        ON base.result_unit_code = ur.CONVERT_FROM_UNIT
    CROSS JOIN canonical_unit cu
    CROSS JOIN value_bounds vb
),

validated AS (
    SELECT
        *,
        CASE
            WHEN unit_status = 'excluded' OR unit_status = 'unknown' THEN NULL
            WHEN numeric_value BETWEEN lower_limit AND upper_limit THEN numeric_value
            ELSE NULL
        END AS inferred_value,

        CASE
            WHEN unit_status = 'excluded' OR unit_status = 'unknown' THEN 'NONE'
            WHEN numeric_value BETWEEN lower_limit AND upper_limit THEN
                CASE
                    WHEN is_canonical THEN 'VERY_HIGH'
                    ELSE 'HIGH'
                END
            ELSE 'NONE'
        END AS confidence,

        CASE
            WHEN unit_status = 'excluded' OR unit_status = 'unknown' THEN NULL
            WHEN numeric_value BETWEEN lower_limit AND upper_limit THEN canonical_unit_code
            ELSE NULL
        END AS inferred_unit,

        CASE
            WHEN unit_status = 'excluded' THEN 'Excluded unit on this measurement type'
            WHEN unit_status = 'unknown' THEN 'Non-accepted unit on percentage measurement'
            WHEN numeric_value BETWEEN lower_limit AND upper_limit THEN
                CASE
                    WHEN is_canonical THEN 'No value or unit conversion necessary'
                    ELSE 'No value conversion, unit was non-standard but numerically equivalent'
                END
            ELSE 'Value out of valid percentage range'
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
    CASE WHEN inferred_unit IS NULL THEN NULL
         ELSE inferred_unit IS DISTINCT FROM original_result_unit_code
    END AS unit_was_changed,
    conversion_reason,
    confidence,
    is_negative,
    is_extreme_outlier,
    is_valid_eosinophil,
    CASE
        WHEN inferred_value IS NULL OR confidence = 'NONE' THEN 'Invalid'
        WHEN inferred_value <= 5 THEN 'Normal'
        WHEN inferred_value <= 20 THEN 'Elevated'
        WHEN inferred_value <= 80 THEN 'Very Elevated'
        ELSE 'Invalid'
    END AS eosinophil_category
FROM flagged
