-- Registration Point-in-Time Comparison Debug
-- Compares registration counts using different filtering strategies against EMIS list size
-- Helps identify which filter combination best matches the EMIS extract
--
-- Usage: dbt compile -s registration_pit_comparison_debug

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

-- All registration-type episodes active at reference date (old method: SNOMED code only)
method_snomed_code AS (
    SELECT
        eoc.organisation_code_publisher AS practice_ods_code,
        COUNT(DISTINCT ptp.person_id) AS patient_count
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

-- Regular type only (old method: SNOMED code + display)
method_regular_display AS (
    SELECT
        eoc.organisation_code_publisher AS practice_ods_code,
        COUNT(DISTINCT ptp.person_id) AS patient_count
    FROM {{ ref('stg_olids_episode_of_care') }} eoc
    CROSS JOIN emis_extract_date ed
    INNER JOIN patient_to_person ptp ON eoc.patient_id = ptp.patient_id
    LEFT JOIN deceased d ON eoc.patient_id = d.patient_id
    WHERE eoc.episode_type_code = '24531000000104'
        AND eoc.episode_type_source_display = 'Regular'
        AND eoc.episode_of_care_start_date <= ed.reference_date
        AND (eoc.episode_of_care_end_date IS NULL OR eoc.episode_of_care_end_date > ed.reference_date)
        AND (d.death_date_approx IS NULL OR d.death_date_approx > ed.reference_date)
    GROUP BY 1
),

-- Regular source code only (no status filter)
method_regular_source AS (
    SELECT
        eoc.organisation_code_publisher AS practice_ods_code,
        COUNT(DISTINCT ptp.person_id) AS patient_count
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

-- Regular + Registered status (current method)
method_regular_registered AS (
    SELECT
        eoc.organisation_code_publisher AS practice_ods_code,
        COUNT(DISTINCT ptp.person_id) AS patient_count
    FROM {{ ref('stg_olids_episode_of_care') }} eoc
    CROSS JOIN emis_extract_date ed
    INNER JOIN patient_to_person ptp ON eoc.patient_id = ptp.patient_id
    LEFT JOIN deceased d ON eoc.patient_id = d.patient_id
    WHERE eoc.episode_type_source_code = 'Regular'
        AND eoc.episode_status_source_code = 'Registered'
        AND eoc.episode_of_care_start_date <= ed.reference_date
        AND (eoc.episode_of_care_end_date IS NULL OR eoc.episode_of_care_end_date > ed.reference_date)
        AND (d.death_date_approx IS NULL OR d.death_date_approx > ed.reference_date)
    GROUP BY 1
),

emis AS (
    SELECT practice_code AS practice_ods_code, list_size AS emis_count
    FROM {{ ref('stg_emis_list_size') }}
)

SELECT
    e.practice_ods_code,
    e.emis_count,
    m1.patient_count AS snomed_code_only,
    m2.patient_count AS regular_display,
    m3.patient_count AS regular_source,
    m4.patient_count AS regular_registered,

    -- Differences vs EMIS
    m1.patient_count - e.emis_count AS diff_snomed,
    m2.patient_count - e.emis_count AS diff_regular_display,
    m3.patient_count - e.emis_count AS diff_regular_source,
    m4.patient_count - e.emis_count AS diff_regular_registered,

    -- Pct differences
    ROUND(100.0 * (m1.patient_count - e.emis_count) / NULLIF(e.emis_count, 0), 2) AS pct_diff_snomed,
    ROUND(100.0 * (m2.patient_count - e.emis_count) / NULLIF(e.emis_count, 0), 2) AS pct_diff_regular_display,
    ROUND(100.0 * (m3.patient_count - e.emis_count) / NULLIF(e.emis_count, 0), 2) AS pct_diff_regular_source,
    ROUND(100.0 * (m4.patient_count - e.emis_count) / NULLIF(e.emis_count, 0), 2) AS pct_diff_regular_registered,

    -- Pass/fail per method (2% or 5 patients)
    CASE WHEN ABS(100.0 * (m1.patient_count - e.emis_count) / NULLIF(e.emis_count, 0)) < 2
              OR ABS(m1.patient_count - e.emis_count) < 5 THEN 'PASS' ELSE 'FAIL' END AS pass_snomed,
    CASE WHEN ABS(100.0 * (m2.patient_count - e.emis_count) / NULLIF(e.emis_count, 0)) < 2
              OR ABS(m2.patient_count - e.emis_count) < 5 THEN 'PASS' ELSE 'FAIL' END AS pass_regular_display,
    CASE WHEN ABS(100.0 * (m3.patient_count - e.emis_count) / NULLIF(e.emis_count, 0)) < 2
              OR ABS(m3.patient_count - e.emis_count) < 5 THEN 'PASS' ELSE 'FAIL' END AS pass_regular_source,
    CASE WHEN ABS(100.0 * (m4.patient_count - e.emis_count) / NULLIF(e.emis_count, 0)) < 2
              OR ABS(m4.patient_count - e.emis_count) < 5 THEN 'PASS' ELSE 'FAIL' END AS pass_regular_registered

FROM emis e
LEFT JOIN method_snomed_code m1 ON e.practice_ods_code = m1.practice_ods_code
LEFT JOIN method_regular_display m2 ON e.practice_ods_code = m2.practice_ods_code
LEFT JOIN method_regular_source m3 ON e.practice_ods_code = m3.practice_ods_code
LEFT JOIN method_regular_registered m4 ON e.practice_ods_code = m4.practice_ods_code
WHERE e.emis_count > 0
ORDER BY e.emis_count DESC
