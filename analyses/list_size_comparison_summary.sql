-- List Size Comparison Summary
-- High-level comparison of active patient counts across registration methods
--
-- Usage: dbt compile -s list_size_comparison_summary, then execute in Snowflake

WITH prpr_summary AS (
    SELECT
        'Practitioner Role' AS method,
        COUNT(DISTINCT sk_patient_id) AS total_active_persons,
        COUNT(DISTINCT practice_ods_code) AS practices_with_registrations,
        COUNT(DISTINCT CASE WHEN is_latest_registration THEN sk_patient_id END) AS persons_with_latest_reg
    FROM {{ ref('int_patient_registrations') }}
    WHERE is_current_registration = TRUE
),

eoc_summary AS (
    SELECT
        'Episode of Care' AS method,
        COUNT(DISTINCT sk_patient_id) AS total_active_persons,
        COUNT(DISTINCT practice_ods_code) AS practices_with_registrations,
        COUNT(DISTINCT CASE WHEN is_latest_registration THEN sk_patient_id END) AS persons_with_latest_reg
    FROM {{ ref('int_patient_registrations_episode_of_care') }}
    WHERE is_current_registration = TRUE
),

fact_summary AS (
    SELECT
        'Fact Patient' AS method,
        COUNT(DISTINCT sk_patient_id) AS total_active_persons,
        COUNT(DISTINCT practice_ods_code) AS practices_with_registrations,
        COUNT(DISTINCT sk_patient_id) AS persons_with_latest_reg
    FROM {{ ref('stg_fact_patient_factpractice') }}
    WHERE is_current_registration = TRUE
),

combined AS (
    SELECT * FROM prpr_summary
    UNION ALL
    SELECT * FROM eoc_summary
    UNION ALL
    SELECT * FROM fact_summary
),

deceased_impact_eoc AS (
    SELECT
        COUNT(DISTINCT sk_patient_id) AS persons_marked_inactive_by_deceased
    FROM {{ ref('int_patient_registrations_episode_of_care') }}
    WHERE registration_status = 'Historical - Deceased'
        AND (
            registration_end_date IS NULL
            OR registration_end_date > CURRENT_DATE()
        )
)

SELECT
    method,
    total_active_persons,
    practices_with_registrations,
    persons_with_latest_reg,
    CASE
        WHEN method = 'Fact Patient' THEN NULL
        ELSE total_active_persons - (SELECT total_active_persons FROM fact_summary)
    END AS diff_vs_fact,
    CASE
        WHEN method = 'Fact Patient' THEN NULL
        ELSE ROUND(
            (total_active_persons::FLOAT / (SELECT total_active_persons FROM fact_summary) * 100) - 100,
            2
        )
    END AS pct_diff_vs_fact
FROM combined

UNION ALL

SELECT
    'EoC Deceased Impact' AS method,
    persons_marked_inactive_by_deceased AS total_active_persons,
    NULL AS practices_with_registrations,
    NULL AS persons_with_latest_reg,
    NULL AS diff_vs_fact,
    NULL AS pct_diff_vs_fact
FROM deceased_impact_eoc

ORDER BY
    CASE method
        WHEN 'Fact Patient' THEN 1
        WHEN 'Practitioner Role' THEN 2
        WHEN 'Episode of Care' THEN 3
        WHEN 'EoC Deceased Impact' THEN 4
    END
