{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All Low density lipoprotein (LDL) ie BAD cholesterol test results codes. Observable values
Includes ALL persons (active, inactive, deceased). QOF target CHOL004. 
Percentage of patients on the QOF CHD, PAD, or STIA Register, with LDL as ≤ 2.0 mmol/L 
*/

WITH base_observations AS (

    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(6,1)) AS cholesterol_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value

    FROM ({{ get_observations("'LDLCCHOL_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
      AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)

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
    CASE
        WHEN cholesterol_value <= 2.0 THEN 'Met'
        WHEN cholesterol_value > 2.0 THEN 'Not Met'
        ELSE 'Unknown'
    END AS LDL_CVD_Target_Met

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
