-- Registration Episodes Marked as 'Left' but Missing End Date
-- These patients are no longer registered but have no end date recorded,
-- causing them to appear as active in date-range-only filters
--
-- Usage: dbt compile -s registration_left_without_end_date

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

left_no_end AS (
    SELECT
        eoc.record_owner_organisation_code AS practice_ods_code,
        eoc.patient_id,
        ptp.person_id,
        eoc.episode_of_care_start_date,
        eoc.episode_of_care_end_date,
        eoc.episode_type_source_code,
        eoc.episode_status_source_code
    FROM {{ ref('stg_olids_episode_of_care') }} eoc
    INNER JOIN patient_to_person ptp ON eoc.patient_id = ptp.patient_id
    WHERE eoc.episode_type_source_code = 'Regular'
        AND eoc.episode_status_source_code = 'Left'
        AND eoc.episode_of_care_end_date IS NULL
),

-- Also check null-status episodes without end dates
null_status_no_end AS (
    SELECT
        eoc.record_owner_organisation_code AS practice_ods_code,
        eoc.patient_id,
        ptp.person_id,
        eoc.episode_of_care_start_date,
        eoc.episode_of_care_end_date,
        eoc.episode_type_source_code,
        eoc.episode_status_source_code
    FROM {{ ref('stg_olids_episode_of_care') }} eoc
    INNER JOIN patient_to_person ptp ON eoc.patient_id = ptp.patient_id
    WHERE eoc.episode_type_source_code = 'Regular'
        AND eoc.episode_status_source_code IS NULL
        AND eoc.episode_of_care_end_date IS NULL
)

-- Practice-level summary of "Left without end date" episodes
SELECT
    practice_ods_code,
    episode_status_source_code,
    COUNT(*) AS episodes,
    COUNT(DISTINCT person_id) AS distinct_persons,
    MIN(episode_of_care_start_date) AS earliest_start,
    MAX(episode_of_care_start_date) AS latest_start
FROM (
    SELECT * FROM left_no_end
    UNION ALL
    SELECT * FROM null_status_no_end
)
GROUP BY practice_ods_code, episode_status_source_code
ORDER BY episodes DESC
