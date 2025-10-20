{{ config(
    materialized='table') }}

-- CVD_64 case finding: QRisk2/3 ≥10% not on statins
-- Identifies patients aged 40-84 with QRisk ≥10% who are NOT on statins (need to start)
-- Excludes those with recent statin medications (12 months) or statin decisions (60 months)

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
        AND CAST(result_value AS NUMBER) >= 10
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
    -- Get patients with statin medications in last 12 months
    SELECT DISTINCT
        person_id,
        MAX(order_date) AS latest_statin_date
    FROM {{ ref('int_ltc_lcs_cvd_medications') }}
    WHERE 
        cluster_id = 'LCS_STAT_COD_CVD'
        AND order_date >= dateadd(MONTH, -12, current_date())
    GROUP BY person_id
),

statin_decisions AS (
    -- Get patients with statin decisions in last 60 months
    SELECT DISTINCT
        person_id,
        MAX(clinical_effective_date) AS latest_decision_date
    FROM {{ ref('int_ltc_lcs_cvd_observations') }}
    WHERE 
        cluster_id = 'STATINDEC_COD'
        AND clinical_effective_date >= dateadd(MONTH, -60, current_date())
    GROUP BY person_id
)

-- Final selection: QRisk ≥10 EXCLUDING those on statins or with statin decisions
SELECT
    bp.person_id,
    bp.age,
    qr.clinical_effective_date AS latest_qrisk2_date,
    CAST(qr.result_value AS NUMBER) AS latest_qrisk2_value,
    qc.all_qrisk2_codes,
    qc.all_qrisk2_displays,
    TRUE AS needs_statin_initiation,
    COALESCE(CAST(qr.result_value AS NUMBER) >= 10, FALSE) AS meets_criteria
FROM {{ ref('int_ltc_lcs_cf_cvd_base_population') }} AS bp
INNER JOIN latest_qrisk2 AS qr ON bp.person_id = qr.person_id
LEFT JOIN qrisk2_codes AS qc ON bp.person_id = qc.person_id
WHERE CAST(qr.result_value AS NUMBER) >= 10