{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All obstructive sleep apnoea (OSA) diagnosis observations from clinical records.
Uses the terminology-managed OBSTRUCTIVE_SLEEP_APNOEA_COD cluster, defined against
the SNOMED hierarchy << 78275009 |Obstructive sleep apnoea syndrome| (top-level
concept and all descendants).

Clinical Purpose:
- OSA comorbidity identification (e.g. tirzepatide eligibility)

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per diagnosis observation.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.patient_id,

    TRUE AS is_diagnosis_code,
    'Obstructive Sleep Apnoea Diagnosis' AS osa_observation_type

FROM ({{ get_observations("'OBSTRUCTIVE_SLEEP_APNOEA_COD'", source='ECL_CACHE') }}) obs

ORDER BY person_id, clinical_effective_date, id
