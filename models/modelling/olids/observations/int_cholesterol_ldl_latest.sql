{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
LATEST Low density lipoprotein (LDL) ie BAD cholesterol test results codes. Observable values
Includes ALL persons (active, inactive, deceased). QOF target CHOL004. 
Percentage of patients on the QOF CHD, PAD, or STIA Register, with LDL as ≤ 2.0 mmol/L 
*/
SELECT
    person_id,
    ID,
    clinical_effective_date,
    cholesterol_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,

       -- Clinical categorisation (mmol/L)
    LDL_CVD_Target_Met
FROM {{ ref('int_cholesterol_ldl_all') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1