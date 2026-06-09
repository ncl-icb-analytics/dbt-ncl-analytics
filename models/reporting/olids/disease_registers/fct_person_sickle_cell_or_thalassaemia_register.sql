{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Sickle Cell or Thalassaemia Register

Diagnosis-only register for inherited haemoglobinopathies.

Business Logic:
- Presence of a sickle cell (SICKLE_COD) or thalassaemia (THAL_COD) diagnosis = on register
- has_sickle_cell / has_thalassaemia flags indicate which condition(s) are present
- No resolution codes available (lifelong conditions)
- No age restrictions
*/

WITH sickle_thal_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS latest_diagnosis_date,

        -- Register logic: any diagnosis = on register
        COALESCE(MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) IS NOT NULL,
        FALSE) AS has_active_diagnosis,

        -- Sub-condition flags
        COALESCE(MAX(
            CASE WHEN is_sickle_code THEN clinical_effective_date END
        ) IS NOT NULL,
        FALSE) AS has_sickle_cell,
        COALESCE(MAX(
            CASE WHEN is_thalassaemia_code THEN clinical_effective_date END
        ) IS NOT NULL,
        FALSE) AS has_thalassaemia,

        -- Sub-condition dates
        MIN(CASE WHEN is_sickle_code THEN clinical_effective_date END)
            AS earliest_sickle_cell_date,
        MAX(CASE WHEN is_sickle_code THEN clinical_effective_date END)
            AS latest_sickle_cell_date,
        MIN(CASE WHEN is_thalassaemia_code THEN clinical_effective_date END)
            AS earliest_thalassaemia_date,
        MAX(CASE WHEN is_thalassaemia_code THEN clinical_effective_date END)
            AS latest_thalassaemia_date,

        -- Count of diagnoses
        COUNT(CASE WHEN is_diagnosis_code THEN 1 END)
            AS total_diagnoses,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END
        ) AS all_concept_displays

    FROM {{ ref('int_sickle_cell_or_thalassaemia_diagnoses_all') }}
    GROUP BY person_id
)

-- Final selection with person demographics
SELECT
    fd.person_id,
    age.age,
    TRUE AS is_on_register,
    fd.has_sickle_cell,
    fd.has_thalassaemia,
    fd.earliest_diagnosis_date,
    fd.latest_diagnosis_date,
    fd.earliest_sickle_cell_date,
    fd.latest_sickle_cell_date,
    fd.earliest_thalassaemia_date,
    fd.latest_thalassaemia_date,
    fd.total_diagnoses,
    fd.all_concept_codes,
    fd.all_concept_displays

FROM sickle_thal_diagnoses AS fd
INNER JOIN {{ ref('dim_person') }} AS p
    ON fd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON fd.person_id = age.person_id
WHERE fd.has_active_diagnosis = TRUE
