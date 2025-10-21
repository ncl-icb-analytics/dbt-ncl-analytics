{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All atrial fibrillation diagnosis observations from clinical records.
Uses QOF atrial fibrillation cluster IDs:
- AFIB_COD: Atrial fibrillation diagnoses
- AFIBRES_COD: Atrial fibrillation resolved/remission codes

Clinical Purpose:
- QOF atrial fibrillation register data collection
- Stroke risk assessment and anticoagulation monitoring
- Cardiovascular rhythm disorder tracking
- Resolution status monitoring

QOF Context:
AF register includes persons with atrial fibrillation diagnosis codes who have not
been resolved. Complex business rules (age restrictions, resolution logic) applied
in downstream fact models for register inclusion.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per AF observation.
Use this model as input for fct_person_atrial_fibrillation_register.sql which applies QOF business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- AF-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'AFIB_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,
    CASE WHEN obs.cluster_id = 'AFIBRES_COD' THEN TRUE ELSE FALSE END AS is_resolved_code,

    -- AF observation type determination
    CASE
        WHEN obs.cluster_id = 'AFIB_COD' THEN 'AF Diagnosis'
        WHEN obs.cluster_id = 'AFIBRES_COD' THEN 'AF Resolved'
        ELSE 'Unknown'
    END AS af_observation_type

FROM ({{ get_observations("'AFIB_COD', 'AFIBRES_COD'", source='PCD') }}) obs

ORDER BY person_id, clinical_effective_date, id
