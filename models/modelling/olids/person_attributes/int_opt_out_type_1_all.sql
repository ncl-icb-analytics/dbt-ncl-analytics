{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Type 1 opt-out observations (dissent from secondary use of primary care data).
Uses cluster IDs: OPT_OUT_TYPE_1_DISSENT_WITHDRAWAL and OPT_OUT_TYPE_1_DISSENT.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS code_description,
    obs.cluster_id AS source_cluster_id,

    CASE
        WHEN obs.cluster_id = 'OPT_OUT_TYPE_1_DISSENT_WITHDRAWAL' THEN TRUE
        ELSE FALSE
    END AS is_withdrawal,

    CASE
        WHEN obs.cluster_id = 'OPT_OUT_TYPE_1_DISSENT' THEN TRUE
        ELSE FALSE
    END AS is_dissent

FROM ({{ get_observations("'OPT_OUT_TYPE_1_DISSENT_WITHDRAWAL', 'OPT_OUT_TYPE_1_DISSENT'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
ORDER BY person_id, clinical_effective_date DESC
