-- Count of distinct patients with recent valproate prescriptions
-- in the child-bearing age cohort (non-male, age 0-55, last 6 months)
--
-- Note: Registration status intentionally not included
--
-- Usage: dbt compile -s valproate_patient_count, then execute the compiled SQL

WITH child_bearing_age AS (
    SELECT person_id
    FROM {{ ref('dim_person_women_child_bearing_age') }}
    WHERE is_child_bearing_age_0_55 = TRUE
),

recent_valproate AS (
    SELECT person_id
    FROM {{ ref('int_valproate_medications_6m_latest') }}
)

SELECT COUNT(DISTINCT cba.person_id) AS patient_count
FROM child_bearing_age cba
INNER JOIN recent_valproate rv
    ON cba.person_id = rv.person_id
