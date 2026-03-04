{{ config(
    materialized='table') }}

-- CVD_65 case finding: Patients with QRisk ≥10% not on high-intensity statins
-- Identifies patients aged 40-84 with QRISK2 ≥10 who are not on high-intensity statins

WITH qrisk2_patients AS (
    -- Get patients with latest QRISK2 ≥ 10
    SELECT DISTINCT
        obs.person_id,
        bp.age,
        obs.clinical_effective_date AS latest_qrisk2_date,
        obs.result_value AS latest_qrisk2_value
    FROM {{ ref('int_ltc_lcs_cvd_observations') }} AS obs
    INNER JOIN
        {{ ref('int_ltc_lcs_cf_base_population') }} AS bp
        ON obs.person_id = bp.person_id
    WHERE
        obs.cluster_id = 'QRISK2_10YEAR'
        AND obs.result_value >= 10
        AND bp.age >= 40
        AND bp.age < 84
        AND obs.clinical_effective_date = (
            SELECT MAX(clinical_effective_date)
            FROM {{ ref('int_ltc_lcs_cvd_observations') }} AS obs2
            WHERE
                obs2.person_id = obs.person_id
                AND obs2.cluster_id = 'QRISK2_10YEAR'
                AND obs2.result_value >= 10
        )
),

statin_exclusions AS (
-- Get patients with statin allergies/contraindications or recent decisions
    SELECT DISTINCT
        person_id,
        MAX(CASE
            WHEN
                cluster_id IN (
                    'STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED'
                )
                THEN clinical_effective_date
        END) AS latest_statin_allergy_date,
        MAX(CASE
            WHEN cluster_id = 'STATINDEC_COD'
                THEN clinical_effective_date
        END) AS latest_statin_decision_date
    FROM {{ ref('int_ltc_lcs_cvd_observations') }}
    WHERE
        cluster_id IN (
            'STATIN_ALLERGY_ADVERSE_REACTION',
            'STATIN_NOT_INDICATED',
            'STATINDEC_COD'
        )
        AND (
            (
                cluster_id IN (
                    'STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED'
                )
            )
            OR (
                cluster_id = 'STATINDEC_COD'
                AND clinical_effective_date
                >= DATEADD('month', -60, CURRENT_DATE())
            )
        )
    GROUP BY person_id
),

eligible_medication_courses AS (
    -- Include only people on the CVD_65 medication courses valueset
    SELECT DISTINCT
        person_id,
        MAX(order_date) AS latest_eligible_statin_date
    FROM ({{ get_ltc_lcs_medication_orders("with_qrisk210_amd_not_on_a_high_intensity_statin_vs3") }})
    GROUP BY person_id
),

high_risk_review_declined AS (
    -- Exclusion from ICB_CF_CVD_65:
    -- "Cardiovascular disease high risk review declined" in the last 3 years
    SELECT DISTINCT
        person_id
    FROM (
        {{ get_ltc_lcs_observations("with_qrisk210_amd_not_on_a_high_intensity_statin_vs1") }}
    )
    WHERE clinical_effective_date >= DATEADD('year', -3, CURRENT_DATE())
),

eligible_patients AS (
-- QRISK2 ≥ 10 patients on CVD_65 medication courses, no allergies, no recent
-- decisions, excluding prior CVD cohorts
    SELECT
        qp.person_id,
        qp.age,
        qp.latest_qrisk2_date,
        qp.latest_qrisk2_value
    FROM qrisk2_patients AS qp
    INNER JOIN eligible_medication_courses AS meds ON qp.person_id = meds.person_id
    LEFT JOIN statin_exclusions AS se ON qp.person_id = se.person_id
    LEFT JOIN {{ ref('dim_ltc_lcs_cf_cvd_61') }} AS cvd_61 ON qp.person_id = cvd_61.person_id
    LEFT JOIN {{ ref('dim_ltc_lcs_cf_cvd_62') }} AS cvd_62 ON qp.person_id = cvd_62.person_id
    LEFT JOIN {{ ref('dim_ltc_lcs_cf_cvd_63') }} AS cvd_63 ON qp.person_id = cvd_63.person_id
    LEFT JOIN {{ ref('dim_ltc_lcs_cf_cvd_64') }} AS cvd_64 ON qp.person_id = cvd_64.person_id
    LEFT JOIN high_risk_review_declined AS hrrd ON qp.person_id = hrrd.person_id
    WHERE
        se.latest_statin_allergy_date IS NULL  -- No statin allergies
        AND se.latest_statin_decision_date IS NULL  -- No statin decisions
        AND cvd_61.person_id IS NULL
        AND cvd_62.person_id IS NULL
        AND cvd_63.person_id IS NULL
        AND cvd_64.person_id IS NULL
        AND hrrd.person_id IS NULL
),

all_qrisk2_readings AS (
-- Get all QRISK2 readings ≥ 10 for eligible patients
    SELECT
        obs.person_id,
        obs.clinical_effective_date,
        obs.result_value,
        obs.mapped_concept_code,
        obs.mapped_concept_display
    FROM {{ ref('int_ltc_lcs_cvd_observations') }} AS obs
    INNER JOIN eligible_patients ON obs.person_id = eligible_patients.person_id
    WHERE
        obs.cluster_id = 'QRISK2_10YEAR'
        AND obs.result_value >= 10
),

all_qrisk2_codes AS (
-- Aggregate all QRISK2 codes and displays for each person
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (
            ORDER BY mapped_concept_code
        ) AS all_qrisk2_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (
            ORDER BY mapped_concept_display
        ) AS all_qrisk2_displays
    FROM all_qrisk2_readings
    GROUP BY person_id
)

-- Final selection: patients who need high-intensity statins (ensure one row per person)
SELECT
    ep.person_id,
    ep.age,
    TRUE AS needs_high_intensity_statin,  -- All patients in this cohort need high-intensity statins
    ep.latest_qrisk2_date,
    ep.latest_qrisk2_value,
    aqc.all_qrisk2_codes,
    aqc.all_qrisk2_displays
FROM eligible_patients AS ep
LEFT JOIN all_qrisk2_codes AS aqc ON ep.person_id = aqc.person_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY ep.person_id ORDER BY ep.latest_qrisk2_date DESC, ep.latest_qrisk2_value DESC) = 1
