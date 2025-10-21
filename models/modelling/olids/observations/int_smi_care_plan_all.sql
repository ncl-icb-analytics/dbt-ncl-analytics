{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
MH/SMI comprehensive care plan QOF Indicator. Date of the mental health care plan code MHP_COD
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,

    -- All records represent completed MH Care Plan
    TRUE AS is_completed_MH_care_plan,

    -- MH Care Plan  currency flags (standard intervals)
    CASE
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS MH_care_plan_current_12m

FROM ({{ get_observations("'MHP_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
ORDER BY person_id, clinical_effective_date DESC