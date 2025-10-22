{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All rheumatoid arthritis diagnoses from clinical records.
Uses QOF cluster ID RA_COD for all forms of rheumatoid arthritis diagnosis.

Clinical Purpose:
- RA register inclusion for QOF musculoskeletal disease management
- Disease activity monitoring and treatment pathway identification
- Inflammatory arthritis care pathway

QOF Context:
RA register follows simple diagnosis-only pattern - any RA diagnosis code
qualifies for register inclusion. No resolution codes or complex criteria.
This is a lifelong condition register for ongoing disease management.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per RA observation.
Use this model as input for fct_person_rheumatoid_arthritis_register.sql which applies QOF business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- RA-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'RA_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,

    -- Observation type determination
    CASE
        WHEN obs.cluster_id = 'RA_COD' THEN 'Rheumatoid Arthritis Diagnosis'
        ELSE 'Unknown'
    END AS ra_observation_type

FROM ({{ get_observations("'RARTH_COD'", source='PCD') }}) obs

ORDER BY person_id, clinical_effective_date, id
