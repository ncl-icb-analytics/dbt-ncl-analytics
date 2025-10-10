{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All Non-Alcoholic Fatty Liver Disease (NAFLD) diagnosis observations from clinical records.
Currently uses hardcoded SNOMED concept codes as no cluster is available in REFERENCE.

⚠️ TODO: Update with proper cluster ID once NAFLD_COD becomes available in REFERENCE.

Clinical Purpose:
- NAFLD diagnosis tracking
- Liver health assessment
- Potential QOF register development

Note: This should be updated to use get_observations() macro once proper cluster ID is available.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per NAFLD observation.
Use this model as input for fct_person_nafld_register.sql which applies business rules.
*/

SELECT
    o.id AS ID,
    pp.person_id,
    o.clinical_effective_date,
    o.mapped_concept_code AS concept_code,
    o.mapped_concept_display AS concept_display,

    -- Source information
    'HARDCODED_NAFLD' AS source_cluster_id,
    o.patient_id,

    -- NAFLD-specific flags (observation-level only)
    TRUE AS is_diagnosis_code,

    -- Observation type determination
    'NAFLD Diagnosis' AS nafld_observation_type,

    -- Additional clinical context
    (o.result_value)::NUMBER(10, 2) AS numeric_value

FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE
    o.mapped_concept_code IN (
        '197315008',    -- Non-alcoholic fatty liver disease
        '1197739005',   -- NAFLD related code
        '1231824009',   -- NAFLD related code
        '442685003',    -- NAFLD related code
        '722866000',    -- NAFLD related code
        '503681000000108' -- NAFLD related code
    )
    AND o.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id
