{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All hospice involvement observations from ECL source.
Uses cluster IDs: HOSPICE_RESIDENT_COD, HOSPICE_FULLCARE_COD, HOSPICE_ATHOME_COD
(involvement) and HOSPICE_DISCHARGE_COD (resolution).
HOSPICE_COD is excluded as it is the union of the three involvement clusters
and would produce duplicate rows.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS code_description,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'HOSPICE_RESIDENT_COD', 'HOSPICE_FULLCARE_COD', 'HOSPICE_ATHOME_COD', 'HOSPICE_DISCHARGE_COD'", source='ECL_CACHE') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
ORDER BY person_id, clinical_effective_date DESC
