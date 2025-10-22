{{ config(
    materialized='table') }}
-- Intermediate model for LTC LCS Case Finding CVD_61
-- Identifies patients with QRISK2 score ≥20% not on statins (case finding for cardiovascular disease prevention)

WITH statin_medications AS (
    -- Get patients on any statins in last 12 months
    SELECT DISTINCT
        person_id,
        MAX(order_date) AS latest_statin_date
    FROM {{ ref('int_ltc_lcs_cvd_medications') }}
    WHERE
        cluster_id = 'LCS_STAT_COD_CVD'
        AND order_date >= DATEADD('month', -12, CURRENT_DATE())
    GROUP BY person_id
),

statin_exclusions AS (
    -- Get patients with statin allergies/contraindications or recent decisions
    SELECT DISTINCT
        person_id,
        MAX(CASE
            WHEN cluster_id IN ('STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED')
                THEN clinical_effective_date
        END) AS latest_statin_allergy_date,
        MAX(CASE
            WHEN cluster_id = 'STATINDEC_COD'
                THEN clinical_effective_date
        END) AS latest_statin_decision_date
    FROM {{ ref('int_ltc_lcs_cvd_observations') }}
    WHERE
        cluster_id IN ('STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED', 'STATINDEC_COD')
        AND (
            cluster_id IN ('STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED')
            OR (cluster_id = 'STATINDEC_COD' AND clinical_effective_date >= DATEADD('month', -60, CURRENT_DATE()))
        )
    GROUP BY person_id
),

qrisk2_readings AS (
    -- Get all QRISK2 readings with valid values
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display
    FROM {{ ref('int_ltc_lcs_cvd_observations') }}
    WHERE
        cluster_id = 'QRISK2_10YEAR'
        AND result_value IS NOT NULL
        AND CAST(result_value AS NUMBER) > 0
),

latest_qrisk2 AS (
    -- Get the latest QRISK2 reading for each person
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        concept_code,
        concept_display,
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
            AS rn
    FROM qrisk2_readings
    QUALIFY rn = 1
),

qrisk2_codes AS (
    -- Aggregate all QRISK2 codes and displays for each person
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code)
            AS all_qrisk2_codes,
        ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (
            ORDER BY concept_display
        ) AS all_qrisk2_displays
    FROM qrisk2_readings
    GROUP BY person_id
)

-- Final selection
SELECT
    bp.person_id,
    bp.age,
    qr.clinical_effective_date AS latest_qrisk2_date,
    codes.all_qrisk2_codes,
    codes.all_qrisk2_displays,
    COALESCE(CAST(qr.result_value AS NUMBER) >= 20, FALSE) AS has_high_qrisk2,
    CAST(qr.result_value AS NUMBER) AS latest_qrisk2_value,
    COALESCE(CAST(qr.result_value AS NUMBER) >= 20, FALSE) AS meets_criteria
FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS bp
INNER JOIN {{ ref('dim_person_age') }} AS age ON bp.person_id = age.person_id
LEFT JOIN latest_qrisk2 AS qr ON bp.person_id = qr.person_id
LEFT JOIN qrisk2_codes AS codes ON bp.person_id = codes.person_id
LEFT JOIN statin_medications AS sm ON bp.person_id = sm.person_id
LEFT JOIN statin_exclusions AS se ON bp.person_id = se.person_id
WHERE
    age.age BETWEEN 40 AND 84  -- CVD base population age range
    AND CAST(qr.result_value AS NUMBER) >= 20  -- QRISK2 ≥20%
    AND sm.person_id IS NULL  -- Not on statins in last 12 months
    AND se.latest_statin_allergy_date IS NULL  -- No statin allergies
    AND se.latest_statin_decision_date IS NULL  -- No statin decisions in last 60 months
