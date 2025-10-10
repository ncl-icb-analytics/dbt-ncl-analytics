{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Gestational Diabetes register fact table - one row per person.
Applies gestational diabetes register inclusion criteria.

Clinical Purpose:
- Clinical gestational diabetes register for pregnancy care and future diabetes risk
- Pregnancy-related diabetes monitoring
- Postpartum diabetes risk assessment
- Future type 2 diabetes prevention planning
- Clinical register (NOT part of QOF)

Register Criteria (Simple Pattern):
- Any gestational diabetes diagnosis code (GESTDIAB_COD)
- No age restrictions (pregnancy-related)
- No resolution codes (permanent record for risk assessment)
- Important for ongoing diabetes risk monitoring

Includes only active patients as per standard population requirements.
This table provides one row per person for analytical use.
*/

WITH gestational_diabetes_diagnoses AS (
    SELECT
        person_id,

        -- Register inclusion dates
        MIN(
            CASE
                WHEN
                    is_diagnosis_code
                    THEN clinical_effective_date
            END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE
                WHEN
                    is_diagnosis_code
                    THEN clinical_effective_date
            END
        ) AS latest_diagnosis_date,

        -- Episode counts
        COUNT(CASE WHEN is_diagnosis_code THEN 1 END)
            AS total_gestational_diabetes_episodes,

        -- Concept code arrays for traceability
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_code
            END
        )
            AS gestational_diabetes_diagnosis_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        )
            AS gestational_diabetes_diagnosis_displays,

        -- Latest observation details
        ARRAY_AGG(DISTINCT ID) AS all_IDs

    FROM {{ ref('int_gestational_diabetes_diagnoses_all') }}
    GROUP BY person_id
),

register_inclusion AS (
    SELECT
        gd.*,

        -- Register logic: Include if has gestational diabetes diagnosis
        COALESCE(earliest_diagnosis_date IS NOT NULL, FALSE) AS is_on_register,

        -- Clinical interpretation
        CASE
            WHEN earliest_diagnosis_date IS NOT NULL
                THEN 'Gestational diabetes history'
            ELSE 'No gestational diabetes diagnosis'
        END AS gestational_diabetes_status,


    FROM gestational_diabetes_diagnoses AS gd
    INNER JOIN {{ ref('dim_person_active_patients') }} AS ap
        ON gd.person_id = ap.person_id
)

SELECT
    ri.person_id,
    ri.is_on_register,
    ri.gestational_diabetes_status,
    ri.earliest_diagnosis_date,
    ri.latest_diagnosis_date,
    ri.total_gestational_diabetes_episodes,
    ri.gestational_diabetes_diagnosis_codes,
    ri.gestational_diabetes_diagnosis_displays,
    ri.all_IDs

FROM register_inclusion AS ri
WHERE ri.is_on_register = TRUE

ORDER BY ri.earliest_diagnosis_date DESC, ri.person_id ASC
