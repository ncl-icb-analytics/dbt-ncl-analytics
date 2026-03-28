{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All cancer diagnosis observations from clinical records.
Uses QOF cancer cluster IDs:
- CAN_COD: Cancer diagnoses

Clinical Purpose:
- QOF cancer register data collection
- Cancer care pathway monitoring
- Oncology treatment tracking
- Resolution/remission status tracking

QOF Context:
Cancer register includes persons with cancer diagnosis codes who have not
been resolved/in remission. Resolution logic applied in downstream fact models.
No specific age restrictions for cancer register.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per cancer observation.
Use this model as input for fct_person_cancer_register.sql which applies QOF business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.episodicity_concept_id,
    ecm.target_code AS episodicity_code,
    ecm.target_display AS episodicity_display,

    -- Cancer-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'CAN_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,

    -- QOF: CAN_DAT is "latest first or new episode" — exclude reviews/ended
    CASE
        WHEN ecm.source_display IN ('Review', 'Ended', 'Changed', 'Evolved', 'Flare Up') THEN FALSE
        ELSE TRUE
    END AS is_first_or_new_episode,

    -- Cancer observation type determination
    CASE
        WHEN obs.cluster_id = 'CAN_COD' THEN 'Cancer Diagnosis'
        ELSE 'Unknown'
    END AS cancer_observation_type

FROM ({{ get_observations("'CAN_COD'", source='PCD') }}) obs
LEFT JOIN {{ ref('stg_olids_enriched_concept_map') }} ecm
    ON obs.episodicity_concept_id = ecm.source_code_id

ORDER BY person_id, clinical_effective_date, id
