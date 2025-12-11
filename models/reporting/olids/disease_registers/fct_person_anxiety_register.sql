{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Anxiety Register - Clinical Quality Measures**

Simple Register with Resolution Tracking

Business Logic:
- Presence of anxiety diagnosis (ANX_COD) = on register
- Unresolved: latest_anxiety_date > latest_resolved_date OR no resolved code
- No QOF restrictions (no age limits, no date thresholds)

Note:
- Resolution codes available (ANXRES_COD) for tracking resolved episodes
- No specific age restrictions applied
- Latest diagnosis date used for care planning

Clinical Context:
Used for anxiety quality measures including:
- Mental health care pathway monitoring
- Anxiety severity tracking
- Recovery planning and review scheduling
- Psychological therapy access
- Medication management and monitoring
*/

WITH anxiety_diagnoses AS (
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
        ), FALSE) AS has_active_anxiety_diagnosis,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_code
            END
        ) AS all_anxiety_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_anxiety_concept_displays,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_resolved_code THEN concept_code END
        ) AS all_resolved_concept_codes

    FROM {{ ref('int_anxiety_diagnoses_all') }}
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
    ad.all_anxiety_concept_codes,
    ad.all_anxiety_concept_displays,
    ad.all_resolved_concept_codes AS all_anxiety_resolved_concept_codes

FROM anxiety_diagnoses AS ad
INNER JOIN {{ ref('dim_person') }} AS p
    ON ad.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON ad.person_id = age.person_id
WHERE ad.has_active_anxiety_diagnosis = TRUE  -- Only include persons with active anxiety diagnosis

