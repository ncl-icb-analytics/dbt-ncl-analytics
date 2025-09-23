{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All DXA scan observations from clinical records.
Uses QOF DXA cluster IDs:
- DXA_COD: DXA scan procedures
- DXA2_COD: DXA T-score measurements (with numeric results)

Clinical Purpose:
- Osteoporosis diagnostic confirmation
- Bone density monitoring
- T-score assessment for fracture risk
- Support for osteoporosis register eligibility

Key Clinical Information:
- DXA scan procedures (DXA_COD)
- T-score measurements (DXA2_COD) with numeric validation
- T-score ≤ -2.5 indicates osteoporosis
- Used in combination with clinical diagnosis for register inclusion

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for osteoporosis register and bone health analytics.
*/

WITH base_observations AS (

    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        CAST(obs.result_value AS NUMBER(6,2)) AS numeric_value,

        -- Flag different types of DXA observations
        CASE WHEN obs.cluster_id = 'DXA_COD' THEN TRUE ELSE FALSE END AS is_dxa_scan_procedure,
        CASE WHEN obs.cluster_id = 'DXA2_COD' THEN TRUE ELSE FALSE END AS is_dxa_t_score_measurement,

        -- T-score clinical validation and interpretation
        CASE
            WHEN obs.cluster_id = 'DXA2_COD'
                 AND CAST(obs.result_value AS NUMBER(6,2)) IS NOT NULL
                 AND CAST(obs.result_value AS NUMBER(6,2)) BETWEEN -6.0 AND 6.0  -- Clinical range validation
            THEN CAST(obs.result_value AS NUMBER(6,2))
            ELSE NULL
        END AS validated_t_score,

        -- Clinical interpretation of T-score
        CASE
            WHEN obs.cluster_id = 'DXA2_COD'
                 AND CAST(obs.result_value AS NUMBER(6,2)) IS NOT NULL
                 AND CAST(obs.result_value AS NUMBER(6,2)) BETWEEN -6.0 AND 6.0
            THEN CASE
                    WHEN CAST(obs.result_value AS NUMBER(6,2)) <= -2.5 THEN 'Osteoporosis'
                    WHEN CAST(obs.result_value AS NUMBER(6,2)) <= -1.0 THEN 'Osteopenia'
                    ELSE 'Normal'
                END
            ELSE NULL
        END AS t_score_interpretation,

        -- QOF osteoporosis confirmation flag
        CASE
            WHEN obs.cluster_id = 'DXA2_COD'
                 AND CAST(obs.result_value AS NUMBER(6,2)) IS NOT NULL
                 AND CAST(obs.result_value AS NUMBER(6,2)) <= -2.5
            THEN TRUE
            ELSE FALSE
        END AS confirms_osteoporosis_diagnosis

    FROM ({{ get_observations("'DXA_COD', 'DXA2_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
)

SELECT
    person_id,
    ID,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    numeric_value,
    is_dxa_scan_procedure,
    is_dxa_t_score_measurement,
    validated_t_score,
    t_score_interpretation,
    confirms_osteoporosis_diagnosis

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
