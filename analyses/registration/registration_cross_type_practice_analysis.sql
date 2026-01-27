-- Cross-Type Practice Attribution Analysis
-- Tests hypothesis: patients with non-Regular episodes at one practice
-- may have Regular episodes at a different practice, causing practice-level
-- misattribution while keeping the total count close to EMIS
--
-- Usage: dbt compile -s registration_cross_type_practice_analysis

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

-- All registration-type episodes active at reference date
active_episodes AS (
    SELECT
        eoc.id,
        ptp.person_id,
        eoc.record_owner_organisation_code AS practice_ods_code,
        eoc.episode_type_source_code,
        eoc.episode_status_source_code,
        eoc.episode_type_code
    FROM {{ ref('stg_olids_episode_of_care') }} eoc
    CROSS JOIN emis_extract_date ed
    INNER JOIN patient_to_person ptp ON eoc.patient_id = ptp.patient_id
    LEFT JOIN deceased d ON eoc.patient_id = d.patient_id
    WHERE eoc.episode_type_code = '24531000000104'
        AND eoc.episode_of_care_start_date <= ed.reference_date
        AND (eoc.episode_of_care_end_date IS NULL OR eoc.episode_of_care_end_date > ed.reference_date)
        AND (d.death_date_approx IS NULL OR d.death_date_approx > ed.reference_date)
),

-- Classify each person's episode types
person_episode_types AS (
    SELECT
        person_id,
        practice_ods_code,
        MAX(CASE WHEN episode_type_source_code = 'Regular' THEN 1 ELSE 0 END) AS has_regular,
        MAX(CASE WHEN episode_type_source_code != 'Regular' OR episode_type_source_code IS NULL THEN 1 ELSE 0 END) AS has_non_regular,
        LISTAGG(DISTINCT episode_type_source_code, ', ') WITHIN GROUP (ORDER BY episode_type_source_code) AS episode_types
    FROM active_episodes
    GROUP BY person_id, practice_ods_code
),

-- Persons with non-Regular episodes: do they also have Regular at same or different practice?
non_regular_persons AS (
    SELECT DISTINCT person_id
    FROM person_episode_types
    WHERE has_non_regular = 1
),

non_regular_detail AS (
    SELECT
        nr.person_id,
        MAX(CASE WHEN pet.has_regular = 1 THEN pet.practice_ods_code END) AS regular_practice,
        MAX(CASE WHEN pet.has_non_regular = 1 AND pet.has_regular = 0 THEN pet.practice_ods_code END) AS non_regular_only_practice,
        MAX(pet.has_regular) AS has_regular_anywhere,
        COUNT(DISTINCT pet.practice_ods_code) AS distinct_practices
    FROM non_regular_persons nr
    INNER JOIN person_episode_types pet ON nr.person_id = pet.person_id
    GROUP BY nr.person_id
)

-- Summary
SELECT
    CASE
        WHEN has_regular_anywhere = 1 AND non_regular_only_practice IS NOT NULL
            THEN 'Has Regular at one practice + non-Regular at another'
        WHEN has_regular_anywhere = 1 AND non_regular_only_practice IS NULL
            THEN 'Has Regular + non-Regular at same practice'
        WHEN has_regular_anywhere = 0
            THEN 'Only non-Regular episodes (no Regular anywhere)'
    END AS patient_category,
    COUNT(*) AS patient_count,
    COUNT(DISTINCT non_regular_only_practice) AS distinct_non_regular_practices
FROM non_regular_detail
GROUP BY 1
ORDER BY patient_count DESC
