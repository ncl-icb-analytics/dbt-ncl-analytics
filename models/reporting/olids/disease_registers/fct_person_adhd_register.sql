{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
ADHD Register

Register with Resolution Tracking

Business Logic:
- Presence of ADHD diagnosis (ADHD_COD) = on register
- Unresolved: latest ADHD_COD > latest ADHDREM_COD OR no remission code
- No age restrictions
*/

WITH adhd_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS latest_diagnosis_date,
        MAX(
            CASE
                WHEN is_resolved_code THEN clinical_effective_date
            END
        ) AS latest_resolved_date,

        -- Register logic: active diagnosis required (unresolved)
        COALESCE(MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) IS NOT NULL
        AND (
            MAX(
                CASE
                    WHEN is_resolved_code THEN clinical_effective_date
                END
            ) IS NULL
            OR MAX(
                CASE
                    WHEN is_diagnosis_code THEN clinical_effective_date
                END
            )
            > MAX(
                CASE
                    WHEN is_resolved_code THEN clinical_effective_date
                END
            )
        ), FALSE) AS has_active_adhd_diagnosis,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_code
            END
        ) AS all_adhd_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_adhd_concept_displays,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_resolved_code THEN concept_code END
        ) AS all_resolved_concept_codes

    FROM {{ ref('int_adhd_diagnoses_all') }}
    GROUP BY person_id
)

-- Final selection with person demographics
SELECT
    ad.person_id,
    age.age,
    TRUE AS is_on_register,
    ad.earliest_diagnosis_date,
    ad.latest_diagnosis_date,
    ad.latest_resolved_date,
    ad.all_adhd_concept_codes,
    ad.all_adhd_concept_displays,
    ad.all_resolved_concept_codes AS all_adhd_resolved_concept_codes

FROM adhd_diagnoses AS ad
INNER JOIN {{ ref('dim_person') }} AS p
    ON ad.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON ad.person_id = age.person_id
WHERE ad.has_active_adhd_diagnosis = TRUE
