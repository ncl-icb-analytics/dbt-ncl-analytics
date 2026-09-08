{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Anticoagulant medication review records from the PCD cluster ORANTICOAGREVW_COD,
one row per observation. Used for NICE IND169, review of anticoagulation in
atrial fibrillation. Includes ALL persons (active, inactive, deceased) following
intermediate layer principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'ORANTICOAGREVW_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
