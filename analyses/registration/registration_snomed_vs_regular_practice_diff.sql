-- SNOMED vs Regular: Per-Practice Difference
-- Shows where non-Regular episodes inflate or deflate practice counts
-- relative to Regular-only, to understand the practice-level misattribution
--
-- Usage: dbt compile -s registration_snomed_vs_regular_practice_diff

WITH emis_extract_date AS (
    SELECT extract_date AS reference_date
    FROM {{ ref('stg_emis_list_size') }}
    LIMIT 1
),

patient_to_person AS (
    SELECT patient_id, person_id
    FROM {{ ref('stg_olids_patient_person') }}
    WHERE patient_id IS NOT NULL AND person_id IS NOT NULL
),

deceased AS (
    SELECT patient_id, death_date_approx
    FROM {{ ref('int_patient_deceased_status') }}
    WHERE is_deceased = TRUE AND death_date_approx IS NOT NULL
),

-- SNOMED code only (all registration types)
snomed_counts AS (
    SELECT
        eoc.organisation_code_publisher AS practice_ods_code,
        COUNT(DISTINCT ptp.person_id) AS snomed_count
    FROM {{ ref('stg_olids_episode_of_care') }} eoc
    CROSS JOIN emis_extract_date ed
    INNER JOIN patient_to_person ptp ON eoc.patient_id = ptp.patient_id
    LEFT JOIN deceased d ON eoc.patient_id = d.patient_id
    WHERE eoc.episode_type_code = '24531000000104'
        AND eoc.episode_of_care_start_date <= ed.reference_date
        AND (eoc.episode_of_care_end_date IS NULL OR eoc.episode_of_care_end_date > ed.reference_date)
        AND (d.death_date_approx IS NULL OR d.death_date_approx > ed.reference_date)
    GROUP BY 1
),

-- Regular source code only
regular_counts AS (
    SELECT
        eoc.organisation_code_publisher AS practice_ods_code,
        COUNT(DISTINCT ptp.person_id) AS regular_count
    FROM {{ ref('stg_olids_episode_of_care') }} eoc
    CROSS JOIN emis_extract_date ed
    INNER JOIN patient_to_person ptp ON eoc.patient_id = ptp.patient_id
    LEFT JOIN deceased d ON eoc.patient_id = d.patient_id
    WHERE eoc.episode_type_source_code = 'Regular'
        AND eoc.episode_of_care_start_date <= ed.reference_date
        AND (eoc.episode_of_care_end_date IS NULL OR eoc.episode_of_care_end_date > ed.reference_date)
        AND (d.death_date_approx IS NULL OR d.death_date_approx > ed.reference_date)
    GROUP BY 1
),

emis AS (
    SELECT practice_code AS practice_ods_code, list_size AS emis_count
    FROM {{ ref('stg_emis_list_size') }}
    WHERE list_size > 0
)

SELECT
    e.practice_ods_code,
    e.emis_count,
    s.snomed_count,
    r.regular_count,
    s.snomed_count - r.regular_count AS non_regular_inflation,
    s.snomed_count - e.emis_count AS snomed_vs_emis,
    r.regular_count - e.emis_count AS regular_vs_emis,
    -- Which method is closer to EMIS for this practice?
    CASE
        WHEN ABS(s.snomed_count - e.emis_count) < ABS(r.regular_count - e.emis_count)
            THEN 'SNOMED closer'
        WHEN ABS(s.snomed_count - e.emis_count) > ABS(r.regular_count - e.emis_count)
            THEN 'Regular closer'
        ELSE 'Equal'
    END AS closer_method
FROM emis e
LEFT JOIN snomed_counts s ON e.practice_ods_code = s.practice_ods_code
LEFT JOIN regular_counts r ON e.practice_ods_code = r.practice_ods_code
ORDER BY non_regular_inflation DESC NULLS LAST
