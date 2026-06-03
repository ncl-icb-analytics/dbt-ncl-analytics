-- GLP-1 / CVD cohort — PERSON-LEVEL FLAGS
-- Cohort: active registered adults (age >= 18) with latest valid BMI >= 27
--         AND established CVD (on CHD [IHD/MI], stroke/TIA, or PAD register).
-- One row per person with flags for: CVD components, Type 2 diabetes, and
-- current GLP-1 use (semaglutide / other GLP-1 / any GLP-1).
-- Join person_id to dim_person_demographics for further splits (ethnicity, IMD, PCN).
--
-- "Already on" = a GLP-1 order in the last 6 months (is_recent_6m). To use a
-- 12-month window, swap is_recent_6m -> is_recent_12m in the glp1 CTE.
--
-- Usage: dbt compile -s glp1_cvd_cohort_flags, then run the compiled SQL.

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

-- Person-level current GLP-1 use, categorised by active ingredient
glp1 AS (
    SELECT
        person_id,
        MAX(CASE WHEN glp1_drug = 'SEMAGLUTIDE' AND is_recent_6m THEN TRUE ELSE FALSE END) AS on_semaglutide,
        MAX(CASE WHEN glp1_drug <> 'SEMAGLUTIDE' AND is_recent_6m THEN TRUE ELSE FALSE END) AS on_other_glp1,
        MAX(CASE WHEN is_recent_6m THEN TRUE ELSE FALSE END) AS on_any_glp1
    FROM {{ ref('int_glp1_medications_all') }}
    GROUP BY person_id
),

-- Adults with BMI >= 27 who have established CVD
cohort AS (
    SELECT
        d.person_id,
        d.age,
        b.bmi_value,
        chd.person_id IS NOT NULL AS has_chd,
        stroke_tia.person_id IS NOT NULL AS has_stroke_tia,
        pad.person_id IS NOT NULL AS has_pad
    FROM {{ ref('dim_person_demographics') }} d
    INNER JOIN bmi b ON d.person_id = b.person_id
    LEFT JOIN chd ON d.person_id = chd.person_id
    LEFT JOIN stroke_tia ON d.person_id = stroke_tia.person_id
    LEFT JOIN pad ON d.person_id = pad.person_id
    WHERE d.age >= 18
      AND d.is_active = TRUE
      AND (chd.person_id IS NOT NULL OR stroke_tia.person_id IS NOT NULL OR pad.person_id IS NOT NULL)
)

SELECT
    c.person_id,
    c.age,
    c.bmi_value,
    c.has_chd,
    c.has_stroke_tia,
    c.has_pad,
    t2.person_id IS NOT NULL AS has_type2_diabetes,
    COALESCE(g.on_semaglutide, FALSE) AS on_semaglutide,
    COALESCE(g.on_other_glp1, FALSE) AS on_other_glp1,
    COALESCE(g.on_any_glp1, FALSE) AS on_any_glp1,
    -- Convenience combo flag for the headline question
    (t2.person_id IS NOT NULL AND COALESCE(g.on_semaglutide, FALSE)) AS has_t2dm_and_on_semaglutide
FROM cohort c
LEFT JOIN type2_diabetes t2 ON c.person_id = t2.person_id
LEFT JOIN glp1 g ON c.person_id = g.person_id
