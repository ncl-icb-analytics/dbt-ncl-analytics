{{
    config(
        materialized='table',
        tags=['data_quality', 'eosinophil'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Eosinophil count data quality issues.
Captures records from int_eosinophil_count that fail validation for investigation.
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
    is_valid_eosinophil,
    eosinophil_category,

    CASE
        WHEN is_negative THEN 'Negative Value'
        WHEN is_extreme_outlier THEN 'Extreme Outlier (> 100 x10*9/L)'
        WHEN confidence = 'NONE' AND conversion_reason = 'Excluded unit on this measurement type' THEN 'Excluded Unit (' || original_result_unit_code || ')'
        WHEN confidence = 'NONE' AND inferred_value IS NULL THEN 'Value Out of Range After All Conversions'
        WHEN confidence = 'NONE' THEN 'Could Not Determine Valid Value'
        ELSE eosinophil_category
    END AS eosinophil_category_with_issues

FROM {{ ref('int_eosinophil_count') }}

WHERE is_valid_eosinophil = FALSE
   OR confidence = 'NONE'
   OR is_extreme_outlier = TRUE
   OR is_negative = TRUE

ORDER BY person_id, clinical_effective_date DESC
