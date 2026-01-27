-- Registration Point-in-Time Comparison Summary
-- Aggregate pass/fail counts per filtering method
-- Shows which method best matches EMIS list sizes across all practices
--
-- Usage: dbt compile -s registration_pit_comparison_summary

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

-- Method 1: SNOMED code only (original)
m1 AS (
    SELECT
        eoc.record_owner_organisation_code AS practice_ods_code,
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

-- Method 2: SNOMED code + display = 'Regular'
m2 AS (
    SELECT
        eoc.record_owner_organisation_code AS practice_ods_code,
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

-- Method 3: source_code = 'Regular' (no status filter)
m3 AS (
    SELECT
        eoc.record_owner_organisation_code AS practice_ods_code,
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

-- Method 4: source_code = 'Regular' + status = 'Registered'
m4 AS (
    SELECT
        eoc.record_owner_organisation_code AS practice_ods_code,
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
    WHERE list_size > 0
),

practice_results AS (
    SELECT
        e.practice_ods_code,
        e.emis_count,
        m1.patient_count AS cnt_snomed,
        m2.patient_count AS cnt_regular_display,
        m3.patient_count AS cnt_regular_source,
        m4.patient_count AS cnt_regular_registered
    FROM emis e
    LEFT JOIN m1 ON e.practice_ods_code = m1.practice_ods_code
    LEFT JOIN m2 ON e.practice_ods_code = m2.practice_ods_code
    LEFT JOIN m3 ON e.practice_ods_code = m3.practice_ods_code
    LEFT JOIN m4 ON e.practice_ods_code = m4.practice_ods_code
)

SELECT
    'snomed_code_only' AS method,
    COUNT(*) AS total_practices,
    SUM(CASE WHEN ABS(100.0 * (cnt_snomed - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(cnt_snomed - emis_count) < 5 THEN 1 ELSE 0 END) AS practices_pass,
    SUM(emis_count) AS total_emis,
    SUM(cnt_snomed) AS total_olids,
    SUM(cnt_snomed) - SUM(emis_count) AS total_diff,
    ROUND(100.0 * (SUM(cnt_snomed) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2) AS total_pct_diff
FROM practice_results

UNION ALL

SELECT
    'regular_display' AS method,
    COUNT(*),
    SUM(CASE WHEN ABS(100.0 * (cnt_regular_display - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(cnt_regular_display - emis_count) < 5 THEN 1 ELSE 0 END),
    SUM(emis_count),
    SUM(cnt_regular_display),
    SUM(cnt_regular_display) - SUM(emis_count),
    ROUND(100.0 * (SUM(cnt_regular_display) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2)
FROM practice_results

UNION ALL

SELECT
    'regular_source' AS method,
    COUNT(*),
    SUM(CASE WHEN ABS(100.0 * (cnt_regular_source - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(cnt_regular_source - emis_count) < 5 THEN 1 ELSE 0 END),
    SUM(emis_count),
    SUM(cnt_regular_source),
    SUM(cnt_regular_source) - SUM(emis_count),
    ROUND(100.0 * (SUM(cnt_regular_source) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2)
FROM practice_results

UNION ALL

SELECT
    'regular_registered' AS method,
    COUNT(*),
    SUM(CASE WHEN ABS(100.0 * (cnt_regular_registered - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(cnt_regular_registered - emis_count) < 5 THEN 1 ELSE 0 END),
    SUM(emis_count),
    SUM(cnt_regular_registered),
    SUM(cnt_regular_registered) - SUM(emis_count),
    ROUND(100.0 * (SUM(cnt_regular_registered) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2)
FROM practice_results

ORDER BY method
