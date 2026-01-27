-- Registration Episode Status Breakdown
-- Shows distribution of episode_type_source_code and episode_status_source_code
-- combinations to understand what types of registration episodes exist in the data
--
-- Usage: dbt compile -s registration_episode_status_breakdown

WITH episode_summary AS (
    SELECT
        eoc.episode_type_source_code,
        eoc.episode_type_source_display,
        eoc.episode_status_source_code,
        eoc.episode_status_source_display,
        eoc.episode_type_code,
        eoc.episode_type_display,
        COUNT(*) AS episode_count,
        COUNT(DISTINCT eoc.patient_id) AS distinct_patients,
        SUM(CASE WHEN eoc.episode_of_care_end_date IS NULL THEN 1 ELSE 0 END) AS null_end_date_count,
        SUM(CASE WHEN eoc.episode_of_care_end_date IS NOT NULL THEN 1 ELSE 0 END) AS has_end_date_count,
        MIN(eoc.episode_of_care_start_date) AS earliest_start,
        MAX(eoc.episode_of_care_start_date) AS latest_start
    FROM {{ ref('stg_olids_episode_of_care') }} eoc
    WHERE eoc.episode_of_care_start_date IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    episode_type_source_code,
    episode_type_source_display,
    episode_status_source_code,
    episode_status_source_display,
    episode_type_code,
    episode_type_display,
    episode_count,
    distinct_patients,
    null_end_date_count,
    has_end_date_count,
    ROUND(100.0 * null_end_date_count / episode_count, 1) AS pct_null_end_date,
    earliest_start,
    latest_start
FROM episode_summary
ORDER BY episode_count DESC
