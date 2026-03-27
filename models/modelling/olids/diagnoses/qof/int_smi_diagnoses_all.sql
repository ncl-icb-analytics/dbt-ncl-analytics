{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All serious mental illness (SMI) diagnosis observations from clinical records.
Uses MH_COD cluster for mental health diagnoses (schizophrenia, bipolar disorder, other psychoses).

Per QOF MH001 spec, MH1_REG is "ever diagnosed" with no remission exclusion.
MHREM_COD is not used — the register includes all patients with MH_DAT != Null.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    TRUE AS is_diagnosis_code,
    FALSE AS is_resolved_code,
    'SMI Diagnosis' AS smi_observation_type

FROM ({{ get_observations("'MH_COD'", source='PCD') }}) obs

ORDER BY person_id, clinical_effective_date, id
