{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Records that limit oral anticoagulant or DOAC treatment, from PCD clusters, one
row per observation. exception_type separates:
- ANTICOAGULANT_CONTRAINDICATED: TXORANTICOAG_COD (persisting: allergy, adverse
  reaction, not tolerated, contraindicated for any oral anticoagulant) and
  TXORANTICOAGGENCON_COD (general anticoagulation contraindicated or not tolerated)
- ANTICOAGULANT_DECLINED: ORANTICOAGDEC_COD
- DOAC_CONTRAINDICATED: DOACCON_COD
- DOAC_DECLINED: DOACDEC_COD
- DOAC_NOT_INDICATED: DOACNI_COD
NICE treats contraindications as persisting and declines as expiring; consumers
apply the window. Includes ALL persons (active, inactive, deceased) following
intermediate layer principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    CASE obs.cluster_id
        WHEN 'TXORANTICOAG_COD' THEN 'ANTICOAGULANT_CONTRAINDICATED'
        WHEN 'TXORANTICOAGGENCON_COD' THEN 'ANTICOAGULANT_CONTRAINDICATED'
        WHEN 'ORANTICOAGDEC_COD' THEN 'ANTICOAGULANT_DECLINED'
        WHEN 'DOACCON_COD' THEN 'DOAC_CONTRAINDICATED'
        WHEN 'DOACDEC_COD' THEN 'DOAC_DECLINED'
        WHEN 'DOACNI_COD' THEN 'DOAC_NOT_INDICATED'
    END AS exception_type,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'TXORANTICOAG_COD', 'TXORANTICOAGGENCON_COD', 'ORANTICOAGDEC_COD', 'DOACCON_COD', 'DOACDEC_COD', 'DOACNI_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
