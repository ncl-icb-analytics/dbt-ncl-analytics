{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Cardiovascular risk assessment records from the PCD cluster CVDRISKASS_COD, one
row per observation: QRISK, QRISK2, QRISK3, JBS and Framingham scores plus
"assessment done" codes. Complements int_qrisk_all, which holds QRISK scores with
a numeric value; this model counts an assessment whether or not a score value
was recorded. Includes ALL persons (active, inactive, deceased) following
intermediate layer principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    TRY_CAST(obs.result_value AS FLOAT) AS risk_score_value,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'CVDRISKASS_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
