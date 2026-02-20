{{
    config(
        materialized='table',
        tags=['data_quality', 'eosinophil'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Eosinophil percentage data quality issues.
Captures records from int_eosinophil_percentage that fail validation for investigation.
*/

SELECT
    person_id,
    id,
    clinical_effective_date,
    concept_code,
    code_description,
    original_result_value,
    original_result_unit_display,
    original_result_unit_code,
    inferred_value,
    inferred_unit,
    confidence,
    conversion_reason,
    value_was_converted,
    unit_was_changed,
    is_negative,
    is_extreme_outlier,
    is_implausible,
    is_valid_eosinophil,
    eosinophil_category,

    CASE
        WHEN is_negative THEN 'Negative Value'
        WHEN is_extreme_outlier THEN 'Extreme Outlier (> 80%)'
        WHEN confidence = 'NONE' AND original_result_unit_code = '10*9/L' THEN 'Excluded Unit (10*9/L on percentage code)'
        WHEN confidence = 'NONE' THEN 'Non-Accepted Unit on Percentage Measurement'
        ELSE eosinophil_category
    END AS eosinophil_category_with_issues

FROM {{ ref('int_eosinophil_percentage') }}

WHERE is_valid_eosinophil = FALSE
   OR confidence = 'NONE'
   OR is_extreme_outlier = TRUE
   OR is_negative = TRUE

ORDER BY person_id, clinical_effective_date DESC
