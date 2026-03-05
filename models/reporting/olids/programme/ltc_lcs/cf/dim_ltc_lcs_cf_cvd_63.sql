{{ config(
    materialized='table') }}
-- Intermediate model for LTC LCS Case Finding CVD_63
-- Identifies patients with QRISK2 ≥10% on statins (within last 12 months)
-- with non-HDL cholesterol > 2.5 (statin review needed)

WITH qrisk2_readings AS (
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
),

statin_medications AS (
    -- Get statin medications in last 12 months
    SELECT
        person_id,
        order_date,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display
    FROM {{ ref('int_ltc_lcs_cvd_medications') }}
    WHERE
        cluster_id = 'STAT_COD'
        AND order_date >= DATEADD(MONTH, -12, CURRENT_DATE())
),

latest_statin AS (
    -- Get the latest statin medication for each person
    SELECT
        person_id,
        order_date,
        concept_code,
        concept_display,
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY order_date DESC)
            AS rn
    FROM statin_medications
    QUALIFY rn = 1
),

statin_codes AS (
    -- Aggregate all statin codes and displays for each person (within 12 months)
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code)
            AS all_statin_codes,
        ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (
            ORDER BY concept_display
        ) AS all_statin_displays
    FROM statin_medications
    GROUP BY person_id
),

statin_exclusions AS (
    -- Exclude statin adverse reaction / not indicated (CVD_63_BASE rule)
    SELECT DISTINCT
        person_id
    FROM {{ ref('int_ltc_lcs_cvd_observations') }}
    WHERE cluster_id IN ('STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED')
),

non_hdl_readings AS (
    -- Get all non-HDL cholesterol readings with valid values
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display
    FROM {{ ref('int_ltc_lcs_cvd_observations') }}
    WHERE
        cluster_id = 'NON_HDL_CHOLESTEROL'
        AND result_value IS NOT NULL
        AND CAST(result_value AS NUMBER) > 0
),

latest_non_hdl AS (
    -- Get the latest non-HDL reading for each person
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        concept_code,
        concept_display,
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
            AS rn
    FROM non_hdl_readings
    QUALIFY rn = 1
),

non_hdl_codes AS (
    -- Aggregate all non-HDL codes and displays for each person
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code)
            AS all_non_hdl_codes,
        ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (
            ORDER BY concept_display
        ) AS all_non_hdl_displays
    FROM non_hdl_readings
    GROUP BY person_id
),

high_risk_review_declined AS (
    -- Exclusion from ICB_CF_CVD_63:
    -- "Cardiovascular disease high risk review declined" in the last 3 years
    SELECT DISTINCT
        person_id
    FROM (
        {{ get_ltc_lcs_observations("qrisk2_10_on_a_statin_and_a_non_hdl_25_vs1") }}
    )
    WHERE clinical_effective_date >= DATEADD('year', -3, CURRENT_DATE())
)

-- Final selection
SELECT
    bp.person_id,
    bp.age,
    qr.clinical_effective_date AS latest_qrisk2_date,
    CAST(qr.result_value AS NUMBER) AS latest_qrisk2_value,
    qc.all_qrisk2_codes,
    qc.all_qrisk2_displays,
    sm.order_date AS latest_statin_date,
    nh.clinical_effective_date AS latest_non_hdl_date,
    sc.all_statin_codes,
    sc.all_statin_displays,
    nhc.all_non_hdl_codes,
    nhc.all_non_hdl_displays,
    COALESCE(
        CAST(qr.result_value AS NUMBER) >= 10
        AND sm.person_id IS NOT NULL
        AND CAST(nh.result_value AS NUMBER) > 2.5,
        FALSE
    ) AS needs_statin_review,
    CAST(nh.result_value AS NUMBER) AS latest_non_hdl_value,
    COALESCE(
        CAST(qr.result_value AS NUMBER) >= 10
        AND sm.person_id IS NOT NULL
        AND CAST(nh.result_value AS NUMBER) > 2.5,
        FALSE
    ) AS meets_criteria
FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS bp
INNER JOIN {{ ref('dim_person_age') }} AS age ON bp.person_id = age.person_id
LEFT JOIN latest_qrisk2 AS qr ON bp.person_id = qr.person_id
LEFT JOIN qrisk2_codes AS qc ON bp.person_id = qc.person_id
LEFT JOIN latest_statin AS sm ON bp.person_id = sm.person_id
LEFT JOIN statin_codes AS sc ON bp.person_id = sc.person_id
LEFT JOIN latest_non_hdl AS nh ON bp.person_id = nh.person_id
LEFT JOIN non_hdl_codes AS nhc ON bp.person_id = nhc.person_id
LEFT JOIN statin_exclusions AS se ON bp.person_id = se.person_id
LEFT JOIN {{ ref('dim_ltc_lcs_cf_cvd_61') }} AS cvd_61 ON bp.person_id = cvd_61.person_id
LEFT JOIN {{ ref('dim_ltc_lcs_cf_cvd_62') }} AS cvd_62 ON bp.person_id = cvd_62.person_id
LEFT JOIN high_risk_review_declined AS hrrd ON bp.person_id = hrrd.person_id
WHERE
    age.age >= 40 AND age.age < 84  -- CVD age range
    AND se.person_id IS NULL  -- No statin allergy / contraindication
    AND cvd_61.person_id IS NULL  -- Exclude CVD_61 cohort
    AND cvd_62.person_id IS NULL  -- Exclude CVD_62 cohort
    AND CAST(qr.result_value AS NUMBER) >= 10  -- Must have QRISK2 ≥10%
    AND sm.person_id IS NOT NULL  -- Must be on statins
    AND CAST(nh.result_value AS NUMBER) > 2.5  -- Must have elevated non-HDL cholesterol
    AND hrrd.person_id IS NULL  -- No high CVD risk review decline in last 3 years
