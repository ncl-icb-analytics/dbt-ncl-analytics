-- Multiple Current Registrations DQ Analysis
-- Explores the ~90K persons with multiple active registration episodes
-- Breaks down by cause: Left-without-end-date, NULL status, genuine overlaps
--
-- Usage: dbt compile -s registration_multiple_current_dq

WITH patient_to_person AS (
    SELECT patient_id, person_id
    FROM {{ ref('stg_olids_patient_person') }}
    WHERE patient_id IS NOT NULL AND person_id IS NOT NULL
),

regular_episodes AS (
    SELECT
        eoc.id,
        eoc.patient_id,
        ptp.person_id,
        eoc.organisation_code_publisher AS practice_ods_code,
        eoc.episode_of_care_start_date,
        eoc.episode_of_care_end_date,
        eoc.episode_type_source_code,
        eoc.episode_status_source_code
    FROM {{ ref('stg_olids_episode_of_care') }} eoc
    INNER JOIN patient_to_person ptp ON eoc.patient_id = ptp.patient_id
    WHERE eoc.episode_type_source_code = 'Regular'
),

-- Episodes that appear "current" (no end date or future end date)
current_episodes AS (
    SELECT
        *,
        CASE
            WHEN episode_status_source_code = 'Registered' AND episode_of_care_end_date IS NULL
                THEN 'Registered (active)'
            WHEN episode_status_source_code = 'Left' AND episode_of_care_end_date IS NULL
                THEN 'Left without end date'
            WHEN episode_status_source_code IS NULL AND episode_of_care_end_date IS NULL
                THEN 'NULL status, no end date'
            WHEN episode_of_care_end_date > CURRENT_DATE()
                THEN 'Future end date'
            ELSE 'Other'
        END AS current_reason
    FROM regular_episodes
    WHERE episode_of_care_end_date IS NULL
       OR episode_of_care_end_date > CURRENT_DATE()
),

-- Persons with multiple "current" episodes
persons_multiple AS (
    SELECT person_id, COUNT(*) AS current_episode_count
    FROM current_episodes
    GROUP BY person_id
    HAVING COUNT(*) > 1
),

-- Detail for those persons
detail AS (
    SELECT
        ce.*,
        pm.current_episode_count
    FROM current_episodes ce
    INNER JOIN persons_multiple pm ON ce.person_id = pm.person_id
),

-- Per-person reason combinations
person_reasons AS (
    SELECT
        person_id,
        current_episode_count,
        LISTAGG(DISTINCT current_reason, ' + ') WITHIN GROUP (ORDER BY current_reason) AS reason_combination
    FROM detail
    GROUP BY person_id, current_episode_count
),

-- Summary: how many multiple-registration persons by combination of reasons
summary AS (
    SELECT
        current_episode_count,
        reason_combination,
        COUNT(DISTINCT person_id) AS distinct_persons,
        SUM(current_episode_count) AS total_episodes
    FROM person_reasons
    GROUP BY current_episode_count, reason_combination
)

SELECT *
FROM summary
ORDER BY distinct_persons DESC
