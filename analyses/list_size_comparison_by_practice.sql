-- List Size Comparison by Practice
-- Compares active patient counts per practice across three methods:
-- 1. Practitioner Role method (int_patient_registrations)
-- 2. Episode of Care method (int_patient_registrations_episode_of_care)
-- 3. Fact Patient (baseline for comparison)
--
-- Usage: dbt compile -s list_size_comparison_by_practice, then execute in Snowflake

WITH prpr_list_sizes AS (
    SELECT
        practice_ods_code,
        practice_name,
        COUNT(DISTINCT sk_patient_id) AS active_persons,
        'Practitioner Role' AS method
    FROM {{ ref('int_patient_registrations') }}
    WHERE is_current_registration = TRUE
    GROUP BY practice_ods_code, practice_name
),

eoc_list_sizes AS (
    SELECT
        practice_ods_code,
        practice_name,
        COUNT(DISTINCT sk_patient_id) AS active_persons,
        'Episode of Care' AS method
    FROM {{ ref('int_patient_registrations_episode_of_care') }}
    WHERE is_current_registration = TRUE
    GROUP BY practice_ods_code, practice_name
),

fact_list_sizes AS (
    SELECT
        practice_ods_code,
        practice_name,
        COUNT(DISTINCT sk_patient_id) AS active_persons,
        'Fact Patient' AS method
    FROM {{ ref('stg_fact_patient_factpractice') }}
    WHERE is_current_registration = TRUE
    GROUP BY practice_ods_code, practice_name
),

combined_list_sizes AS (
    SELECT * FROM prpr_list_sizes
    UNION ALL
    SELECT * FROM eoc_list_sizes
    UNION ALL
    SELECT * FROM fact_list_sizes
),

all_practices AS (
    -- Get distinct practices from both OLIDS methods with a single consistent name per ODS code
    SELECT DISTINCT
        practice_ods_code,
        FIRST_VALUE(practice_name) OVER (
            PARTITION BY practice_ods_code
            ORDER BY practice_name
        ) AS practice_name
    FROM (
        SELECT DISTINCT practice_ods_code, practice_name FROM prpr_list_sizes
        UNION
        SELECT DISTINCT practice_ods_code, practice_name FROM eoc_list_sizes
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY practice_ods_code ORDER BY practice_name) = 1
),

pivoted_comparison AS (
    SELECT
        practices.practice_ods_code,
        practices.practice_name,
        prpr.active_persons AS prpr_active_persons,
        eoc.active_persons AS eoc_active_persons,
        fact.active_persons AS fact_active_persons
    FROM all_practices practices
    LEFT JOIN prpr_list_sizes prpr
        ON practices.practice_ods_code = prpr.practice_ods_code
    LEFT JOIN eoc_list_sizes eoc
        ON practices.practice_ods_code = eoc.practice_ods_code
    LEFT JOIN fact_list_sizes fact
        ON practices.practice_ods_code = fact.practice_ods_code
)

SELECT
    practice_ods_code,
    practice_name,

    -- Counts per method
    COALESCE(prpr_active_persons, 0) AS prpr_count,
    COALESCE(eoc_active_persons, 0) AS eoc_count,
    COALESCE(fact_active_persons, 0) AS fact_count,

    -- Differences vs Fact Patient
    COALESCE(prpr_active_persons, 0) - COALESCE(fact_active_persons, 0) AS prpr_vs_fact_diff,
    COALESCE(eoc_active_persons, 0) - COALESCE(fact_active_persons, 0) AS eoc_vs_fact_diff,

    -- Percentage vs Fact Patient
    ROUND(
        (COALESCE(prpr_active_persons, 0)::FLOAT / NULLIF(fact_active_persons, 0) * 100) - 100,
        2
    ) AS prpr_vs_fact_pct_diff,
    ROUND(
        (COALESCE(eoc_active_persons, 0)::FLOAT / NULLIF(fact_active_persons, 0) * 100) - 100,
        2
    ) AS eoc_vs_fact_pct_diff,

    -- Difference between methods
    COALESCE(eoc_active_persons, 0) - COALESCE(prpr_active_persons, 0) AS eoc_vs_prpr_diff,

    -- Flag significant discrepancies
    CASE
        WHEN fact_active_persons IS NULL THEN 'Missing from Fact Patient'
        WHEN prpr_active_persons IS NULL AND eoc_active_persons IS NULL THEN 'Missing from OLIDS'
        WHEN ABS(COALESCE(prpr_active_persons, 0) - COALESCE(fact_active_persons, 0)) > (fact_active_persons * 0.1)
            THEN 'PRPR >10% diff from Fact'
        WHEN ABS(COALESCE(eoc_active_persons, 0) - COALESCE(fact_active_persons, 0)) > (fact_active_persons * 0.1)
            THEN 'EoC >10% diff from Fact'
        ELSE 'Within tolerance'
    END AS variance_flag

FROM pivoted_comparison
WHERE practice_ods_code IS NOT NULL
ORDER BY
    CASE variance_flag
        WHEN 'Missing from Fact Patient' THEN 1
        WHEN 'Missing from OLIDS' THEN 2
        WHEN 'PRPR >10% diff from Fact' THEN 3
        WHEN 'EoC >10% diff from Fact' THEN 4
        ELSE 5
    END,
    ABS(COALESCE(eoc_active_persons, 0) - COALESCE(fact_active_persons, 0)) DESC
