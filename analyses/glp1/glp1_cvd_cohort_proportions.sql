-- GLP-1 / CVD cohort — AGGREGATE PROPORTIONS
-- Denominator: active registered adults (age >= 18) with latest valid BMI >= 27
--              AND established CVD (on CHD [IHD/MI], stroke/TIA, or PAD register).
-- Answers:
--   - proportion with Type 2 diabetes AND already on semaglutide
--   - proportion already on semaglutide (any indication)
--   - proportion already on another (non-semaglutide) GLP-1
--
-- "Already on" = a GLP-1 order in the last 6 months (is_recent_6m). To use a
-- 12-month window, swap is_recent_6m -> is_recent_12m in the glp1 CTE.
--
-- Usage: dbt compile -s glp1_cvd_cohort_proportions, then run the compiled SQL.

WITH bmi AS (
    SELECT person_id, bmi_value
    FROM {{ ref('int_bmi_latest') }}
    WHERE bmi_value >= 27
),

chd AS (
    SELECT person_id FROM {{ ref('fct_person_chd_register') }} WHERE is_on_register = TRUE
),
stroke_tia AS (
    SELECT person_id FROM {{ ref('fct_person_stroke_tia_register') }} WHERE is_on_register = TRUE
),
pad AS (
    SELECT person_id FROM {{ ref('fct_person_pad_register') }} WHERE is_on_register = TRUE
),

type2_diabetes AS (
    SELECT person_id
    FROM {{ ref('fct_person_diabetes_register') }}
    WHERE is_on_register = TRUE AND diabetes_type = 'Type 2'
),

glp1 AS (
    SELECT
        person_id,
        MAX(CASE WHEN glp1_drug = 'SEMAGLUTIDE' AND is_recent_6m THEN TRUE ELSE FALSE END) AS on_semaglutide,
        MAX(CASE WHEN glp1_drug <> 'SEMAGLUTIDE' AND is_recent_6m THEN TRUE ELSE FALSE END) AS on_other_glp1,
        MAX(CASE WHEN is_recent_6m THEN TRUE ELSE FALSE END) AS on_any_glp1
    FROM {{ ref('int_glp1_medications_all') }}
    GROUP BY person_id
),

cohort AS (
    SELECT d.person_id
    FROM {{ ref('dim_person_demographics') }} d
    INNER JOIN bmi b ON d.person_id = b.person_id
    LEFT JOIN chd ON d.person_id = chd.person_id
    LEFT JOIN stroke_tia ON d.person_id = stroke_tia.person_id
    LEFT JOIN pad ON d.person_id = pad.person_id
    WHERE d.age >= 18
      AND d.is_active = TRUE
      AND (chd.person_id IS NOT NULL OR stroke_tia.person_id IS NOT NULL OR pad.person_id IS NOT NULL)
),

flagged AS (
    SELECT
        c.person_id,
        t2.person_id IS NOT NULL AS has_type2_diabetes,
        COALESCE(g.on_semaglutide, FALSE) AS on_semaglutide,
        COALESCE(g.on_other_glp1, FALSE) AS on_other_glp1,
        COALESCE(g.on_any_glp1, FALSE) AS on_any_glp1
    FROM cohort c
    LEFT JOIN type2_diabetes t2 ON c.person_id = t2.person_id
    LEFT JOIN glp1 g ON c.person_id = g.person_id
),

agg AS (
    SELECT
        COUNT(*) AS cohort_n,
        COUNT_IF(has_type2_diabetes) AS n_type2_diabetes,
        COUNT_IF(on_semaglutide) AS n_on_semaglutide,
        COUNT_IF(has_type2_diabetes AND on_semaglutide) AS n_t2dm_on_semaglutide,
        COUNT_IF(on_other_glp1) AS n_on_other_glp1,
        COUNT_IF(on_any_glp1) AS n_on_any_glp1
    FROM flagged
)

SELECT
    cohort_n,
    n_type2_diabetes,
    ROUND(100.0 * n_type2_diabetes / NULLIF(cohort_n, 0), 1) AS pct_type2_diabetes,
    n_t2dm_on_semaglutide,
    ROUND(100.0 * n_t2dm_on_semaglutide / NULLIF(cohort_n, 0), 1) AS pct_t2dm_on_semaglutide,
    n_on_semaglutide,
    ROUND(100.0 * n_on_semaglutide / NULLIF(cohort_n, 0), 1) AS pct_on_semaglutide,
    n_on_other_glp1,
    ROUND(100.0 * n_on_other_glp1 / NULLIF(cohort_n, 0), 1) AS pct_on_other_glp1,
    n_on_any_glp1,
    ROUND(100.0 * n_on_any_glp1 / NULLIF(cohort_n, 0), 1) AS pct_on_any_glp1
FROM agg
