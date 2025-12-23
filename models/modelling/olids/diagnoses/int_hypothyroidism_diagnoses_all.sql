{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All hypothyroidism diagnosis observations from clinical records.
Uses hypothyroidism cluster ID:
- THY_COD: Hypothyroidism diagnoses

Clinical Purpose:
- Hypothyroidism register data collection
- Endocrine condition monitoring
- Care pathway tracking
- Medication management support

Clinical Context:
Hypothyroidism register includes persons with hypothyroidism diagnosis codes.
Hypothyroidism is a chronic endocrine condition with no resolution codes.
No age restrictions applied - condition can occur at any age though more common in older adults, particularly women.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per hypothyroidism observation.
Use this model as input for fct_person_hypothyroidism_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Hypothyroidism-specific flags (observation-level only)
    TRUE AS is_diagnosis_code,

    -- Hypothyroidism observation type
    'Hypothyroidism Diagnosis' AS hypothyroidism_observation_type

FROM ({{ get_observations("'THY_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id

