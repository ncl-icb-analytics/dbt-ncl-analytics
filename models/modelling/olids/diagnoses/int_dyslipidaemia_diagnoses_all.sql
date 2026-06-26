{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All dyslipidaemia diagnosis observations from clinical records.
Uses the terminology-managed DYSLIPIDAEMIA_COD cluster, defined against the
SNOMED hierarchy << 48286001 |Disorder of lipoprotein and/or lipid metabolism|
(broad net: includes hyperlipidaemia and hypercholesterolaemia, which sit under
48286001 but not under the narrower 370992007 |Dyslipidaemia|).

Clinical Purpose:
- Dyslipidaemia comorbidity identification (e.g. tirzepatide eligibility)
- Cardiovascular risk assessment support

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
    'Dyslipidaemia Diagnosis' AS dyslipidaemia_observation_type

FROM ({{ get_observations("'DYSLIPIDAEMIA_COD'", source='ECL_CACHE', include_history=true) }}) obs

ORDER BY person_id, clinical_effective_date, id
