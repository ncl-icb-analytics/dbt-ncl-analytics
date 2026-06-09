{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All sickle cell and thalassaemia diagnosis observations from clinical records.
Uses haemoglobinopathy cluster IDs:
- SICKLE_COD: Sickle cell disease diagnoses
- THAL_COD: Thalassaemia diagnoses

No resolution codes available — diagnosis-only register.
No age restrictions applied. A person qualifies via either condition.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per observation.
Use this model as input for fct_person_sickle_cell_or_thalassaemia_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Diagnosis flags (observation-level only)
    TRUE AS is_diagnosis_code,
    CASE WHEN obs.cluster_id = 'SICKLE_COD' THEN TRUE ELSE FALSE END AS is_sickle_code,
    CASE WHEN obs.cluster_id = 'THAL_COD' THEN TRUE ELSE FALSE END AS is_thalassaemia_code,

    -- Observation type determination
    CASE
        WHEN obs.cluster_id = 'SICKLE_COD' THEN 'Sickle Cell Disease Diagnosis'
        WHEN obs.cluster_id = 'THAL_COD' THEN 'Thalassaemia Diagnosis'
        ELSE 'Unknown'
    END AS sickle_or_thalassaemia_observation_type

FROM ({{ get_observations("'SICKLE_COD', 'THAL_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id
