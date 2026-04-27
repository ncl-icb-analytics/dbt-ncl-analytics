-- Registration Filter Variations
-- Tests multiple filtering strategies to find the best EMIS match
--
-- Variations:
-- 1. Regular only (baseline, 139 pass)
-- 2. Regular + Externally Registered
-- 3. Regular + all 24531000000104 SNOMED types except known non-GP (Dermatology, Rheumatology, etc.)
-- 4. Regular, strict > end date (current)
-- 5. Regular, >= end date
-- 6. Regular, exclude Left-with-NULL-end-date
-- 7. Regular, count by patient_id instead of person_id
-- 8. Regular + NULL episode_type_source_code (where SNOMED = 24531000000104)
--
-- Usage: dbt compile -s registration_filter_variations

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

base_episodes AS (
    SELECT
        eoc.id,
        eoc.patient_id,
        ptp.person_id,
        eoc.organisation_code_publisher AS practice_ods_code,
        eoc.episode_of_care_start_date,
        eoc.episode_of_care_end_date,
        eoc.episode_type_source_code,
        eoc.episode_status_source_code,
        eoc.episode_type_code
    FROM {{ ref('stg_olids_episode_of_care') }} eoc
    CROSS JOIN emis_extract_date ed
    INNER JOIN patient_to_person ptp ON eoc.patient_id = ptp.patient_id
    LEFT JOIN deceased d ON eoc.patient_id = d.patient_id
    WHERE eoc.episode_of_care_start_date <= ed.reference_date
        AND eoc.episode_of_care_start_date IS NOT NULL
        AND eoc.patient_id IS NOT NULL
        AND eoc.organisation_id_publisher IS NOT NULL
        AND (d.death_date_approx IS NULL OR d.death_date_approx > ed.reference_date)
),

emis AS (
    SELECT practice_code AS practice_ods_code, list_size AS emis_count
    FROM {{ ref('stg_emis_list_size') }}
    WHERE list_size > 0
),

-- V1: Regular only, strict > (baseline)
v1 AS (
    SELECT practice_ods_code, COUNT(DISTINCT person_id) AS cnt
    FROM base_episodes
    WHERE episode_type_source_code = 'Regular'
        AND (episode_of_care_end_date IS NULL OR episode_of_care_end_date > (SELECT reference_date FROM emis_extract_date))
    GROUP BY 1
),

-- V2: Regular + Externally Registered
v2 AS (
    SELECT practice_ods_code, COUNT(DISTINCT person_id) AS cnt
    FROM base_episodes
    WHERE episode_type_source_code IN ('Regular', 'Externally Registered')
        AND (episode_of_care_end_date IS NULL OR episode_of_care_end_date > (SELECT reference_date FROM emis_extract_date))
    GROUP BY 1
),

-- V3: All SNOMED 24531000000104 except known specialist types
v3 AS (
    SELECT practice_ods_code, COUNT(DISTINCT person_id) AS cnt
    FROM base_episodes
    WHERE episode_type_code = '24531000000104'
        AND (episode_type_source_code IS NULL OR episode_type_source_code NOT IN (
            'Dermatology', 'Rheumatology', 'Minor Surgery', 'Ultrasound',
            'Rehabilitation', 'Child Health Services', 'Acupuncture',
            'Community Registered', 'Yellow Fever', 'Walk-In Patient',
            'Diabetic', 'Pre Registration'
        ))
        AND (episode_of_care_end_date IS NULL OR episode_of_care_end_date > (SELECT reference_date FROM emis_extract_date))
    GROUP BY 1
),

-- V4: Regular, >= end date instead of >
v4 AS (
    SELECT practice_ods_code, COUNT(DISTINCT person_id) AS cnt
    FROM base_episodes
    WHERE episode_type_source_code = 'Regular'
        AND (episode_of_care_end_date IS NULL OR episode_of_care_end_date >= (SELECT reference_date FROM emis_extract_date))
    GROUP BY 1
),

-- V5: Regular, exclude episodes with status=Left and NULL end date
v5 AS (
    SELECT practice_ods_code, COUNT(DISTINCT person_id) AS cnt
    FROM base_episodes
    WHERE episode_type_source_code = 'Regular'
        AND (episode_of_care_end_date IS NULL OR episode_of_care_end_date > (SELECT reference_date FROM emis_extract_date))
        AND NOT (episode_status_source_code = 'Left' AND episode_of_care_end_date IS NULL)
    GROUP BY 1
),

-- V6: Regular, count by patient_id instead of person_id
v6 AS (
    SELECT practice_ods_code, COUNT(DISTINCT patient_id) AS cnt
    FROM base_episodes
    WHERE episode_type_source_code = 'Regular'
        AND (episode_of_care_end_date IS NULL OR episode_of_care_end_date > (SELECT reference_date FROM emis_extract_date))
    GROUP BY 1
),

-- V7: Regular + NULL source code (where SNOMED = 24531000000104)
v7 AS (
    SELECT practice_ods_code, COUNT(DISTINCT person_id) AS cnt
    FROM base_episodes
    WHERE (episode_type_source_code = 'Regular'
           OR (episode_type_source_code IS NULL AND episode_type_code = '24531000000104'))
        AND (episode_of_care_end_date IS NULL OR episode_of_care_end_date > (SELECT reference_date FROM emis_extract_date))
    GROUP BY 1
),

-- V8: Regular + Externally Registered + Private
v8 AS (
    SELECT practice_ods_code, COUNT(DISTINCT person_id) AS cnt
    FROM base_episodes
    WHERE episode_type_source_code IN ('Regular', 'Externally Registered', 'Private')
        AND (episode_of_care_end_date IS NULL OR episode_of_care_end_date > (SELECT reference_date FROM emis_extract_date))
    GROUP BY 1
),

results AS (
    SELECT
        e.practice_ods_code,
        e.emis_count,
        v1.cnt AS v1_regular,
        v2.cnt AS v2_reg_ext,
        v3.cnt AS v3_snomed_excl,
        v4.cnt AS v4_gte_end,
        v5.cnt AS v5_excl_left_null,
        v6.cnt AS v6_patient_id,
        v7.cnt AS v7_reg_null,
        v8.cnt AS v8_reg_ext_priv
    FROM emis e
    LEFT JOIN v1 ON e.practice_ods_code = v1.practice_ods_code
    LEFT JOIN v2 ON e.practice_ods_code = v2.practice_ods_code
    LEFT JOIN v3 ON e.practice_ods_code = v3.practice_ods_code
    LEFT JOIN v4 ON e.practice_ods_code = v4.practice_ods_code
    LEFT JOIN v5 ON e.practice_ods_code = v5.practice_ods_code
    LEFT JOIN v6 ON e.practice_ods_code = v6.practice_ods_code
    LEFT JOIN v7 ON e.practice_ods_code = v7.practice_ods_code
    LEFT JOIN v8 ON e.practice_ods_code = v8.practice_ods_code
)

SELECT
    'V1: Regular only (baseline)' AS method,
    SUM(CASE WHEN ABS(100.0 * (v1_regular - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(v1_regular - emis_count) < 5 THEN 1 ELSE 0 END) AS pass,
    SUM(emis_count) AS total_emis,
    SUM(v1_regular) AS total_olids,
    SUM(v1_regular) - SUM(emis_count) AS total_diff,
    ROUND(100.0 * (SUM(v1_regular) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2) AS pct_diff
FROM results
UNION ALL
SELECT 'V2: Regular + Ext Registered',
    SUM(CASE WHEN ABS(100.0 * (v2_reg_ext - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(v2_reg_ext - emis_count) < 5 THEN 1 ELSE 0 END),
    SUM(emis_count), SUM(v2_reg_ext), SUM(v2_reg_ext) - SUM(emis_count),
    ROUND(100.0 * (SUM(v2_reg_ext) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2)
FROM results
UNION ALL
SELECT 'V3: SNOMED excl specialist',
    SUM(CASE WHEN ABS(100.0 * (v3_snomed_excl - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(v3_snomed_excl - emis_count) < 5 THEN 1 ELSE 0 END),
    SUM(emis_count), SUM(v3_snomed_excl), SUM(v3_snomed_excl) - SUM(emis_count),
    ROUND(100.0 * (SUM(v3_snomed_excl) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2)
FROM results
UNION ALL
SELECT 'V4: Regular, >= end date',
    SUM(CASE WHEN ABS(100.0 * (v4_gte_end - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(v4_gte_end - emis_count) < 5 THEN 1 ELSE 0 END),
    SUM(emis_count), SUM(v4_gte_end), SUM(v4_gte_end) - SUM(emis_count),
    ROUND(100.0 * (SUM(v4_gte_end) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2)
FROM results
UNION ALL
SELECT 'V5: Regular, excl Left+NULL end',
    SUM(CASE WHEN ABS(100.0 * (v5_excl_left_null - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(v5_excl_left_null - emis_count) < 5 THEN 1 ELSE 0 END),
    SUM(emis_count), SUM(v5_excl_left_null), SUM(v5_excl_left_null) - SUM(emis_count),
    ROUND(100.0 * (SUM(v5_excl_left_null) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2)
FROM results
UNION ALL
SELECT 'V6: Regular, patient_id dedup',
    SUM(CASE WHEN ABS(100.0 * (v6_patient_id - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(v6_patient_id - emis_count) < 5 THEN 1 ELSE 0 END),
    SUM(emis_count), SUM(v6_patient_id), SUM(v6_patient_id) - SUM(emis_count),
    ROUND(100.0 * (SUM(v6_patient_id) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2)
FROM results
UNION ALL
SELECT 'V7: Regular + NULL source code',
    SUM(CASE WHEN ABS(100.0 * (v7_reg_null - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(v7_reg_null - emis_count) < 5 THEN 1 ELSE 0 END),
    SUM(emis_count), SUM(v7_reg_null), SUM(v7_reg_null) - SUM(emis_count),
    ROUND(100.0 * (SUM(v7_reg_null) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2)
FROM results
UNION ALL
SELECT 'V8: Regular + Ext Reg + Private',
    SUM(CASE WHEN ABS(100.0 * (v8_reg_ext_priv - emis_count) / NULLIF(emis_count, 0)) < 2
                  OR ABS(v8_reg_ext_priv - emis_count) < 5 THEN 1 ELSE 0 END),
    SUM(emis_count), SUM(v8_reg_ext_priv), SUM(v8_reg_ext_priv) - SUM(emis_count),
    ROUND(100.0 * (SUM(v8_reg_ext_priv) - SUM(emis_count)) / NULLIF(SUM(emis_count), 0), 2)
FROM results

ORDER BY pass DESC, ABS(total_diff) ASC
