{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Complex Adults Cohort v1.0
-- One row per person meeting all cohort criteria:
--   Age >= 18
--   AND (moderate/severe frailty OR 3+ included LTCs)
--   AND (>=2 NEL admissions OR >=3 ED attendances in last 12 months, OR housebound)
-- Includes all persons regardless of registration status; filter is_active = TRUE
-- for the currently registered population.
-- Included LTCs (15): AF, Asthma, CHD, CKD, COPD, Dementia, Depression, Diabetes,
-- Epilepsy, Heart Failure, Hypertension, SMI, Stroke/TIA, Parkinson's, Anxiety.
-- ED attendances count all urgent & emergency care settings from ECDS
-- (Type 1/2 A&E, UTC, WiC, SDEC).
-- 12-month windows are rolling from the build date.

WITH ltc AS (
    SELECT
        person_id,
        BOOLOR_AGG(condition_code = 'AF') AS has_af,
        BOOLOR_AGG(condition_code = 'AST') AS has_asthma,
        BOOLOR_AGG(condition_code = 'CHD') AS has_chd,
        BOOLOR_AGG(condition_code = 'CKD') AS has_ckd,
        BOOLOR_AGG(condition_code = 'COPD') AS has_copd,
        BOOLOR_AGG(condition_code = 'DEM') AS has_dementia,
        BOOLOR_AGG(condition_code = 'DEP') AS has_depression,
        BOOLOR_AGG(condition_code = 'DM') AS has_diabetes,
        BOOLOR_AGG(condition_code = 'EP') AS has_epilepsy,
        BOOLOR_AGG(condition_code = 'HF') AS has_heart_failure,
        BOOLOR_AGG(condition_code = 'HTN') AS has_hypertension,
        BOOLOR_AGG(condition_code = 'SMI') AS has_smi,
        BOOLOR_AGG(condition_code = 'STIA') AS has_stroke_tia,
        BOOLOR_AGG(condition_code = 'PD') AS has_parkinsons,
        BOOLOR_AGG(condition_code = 'ANX') AS has_anxiety,
        COUNT(DISTINCT condition_code) AS ltc_count
    FROM {{ ref('fct_person_ltc_summary') }}
    WHERE condition_code IN (
        'AF', 'AST', 'CHD', 'CKD', 'COPD', 'DEM', 'DEP', 'DM',
        'EP', 'HF', 'HTN', 'SMI', 'STIA', 'PD', 'ANX'
    )
    GROUP BY person_id
)

SELECT
    d.person_id,
    d.sk_patient_id,
    d.is_active,
    d.age,
    d.gender,
    d.practice_code,
    d.practice_name,
    d.borough_registered,
    d.neighbourhood_registered,

    -- Condition flags
    COALESCE(l.has_af, FALSE) AS has_af,
    COALESCE(l.has_asthma, FALSE) AS has_asthma,
    COALESCE(l.has_chd, FALSE) AS has_chd,
    COALESCE(l.has_ckd, FALSE) AS has_ckd,
    COALESCE(l.has_copd, FALSE) AS has_copd,
    COALESCE(l.has_dementia, FALSE) AS has_dementia,
    COALESCE(l.has_depression, FALSE) AS has_depression,
    COALESCE(l.has_diabetes, FALSE) AS has_diabetes,
    COALESCE(l.has_epilepsy, FALSE) AS has_epilepsy,
    COALESCE(l.has_heart_failure, FALSE) AS has_heart_failure,
    COALESCE(l.has_hypertension, FALSE) AS has_hypertension,
    COALESCE(l.has_smi, FALSE) AS has_smi,
    COALESCE(l.has_stroke_tia, FALSE) AS has_stroke_tia,
    COALESCE(l.has_parkinsons, FALSE) AS has_parkinsons,
    COALESCE(l.has_anxiety, FALSE) AS has_anxiety,
    COALESCE(l.ltc_count, 0) AS ltc_count,

    -- Frailty
    f.latest_frailty_severity,
    COALESCE(f.latest_frailty_severity IN ('Moderate', 'Severe'), FALSE)
        AS has_moderate_severe_frailty,

    -- Utilisation
    ZEROIFNULL(ip.apc_nel_12mo) AS nel_admissions_12mo,
    ZEROIFNULL(ae.ae_tot_12mo) AS ed_attendances_12mo,
    COALESCE(h.is_housebound, FALSE) AS is_housebound

FROM {{ ref('dim_person_demographics') }} AS d
LEFT JOIN ltc AS l
    ON d.person_id = l.person_id
LEFT JOIN {{ ref('fct_person_frailty_register') }} AS f
    ON d.person_id = f.person_id
LEFT JOIN {{ ref('dim_person_housebound_status') }} AS h
    ON d.person_id = h.person_id
LEFT JOIN {{ ref('fct_person_sus_ip_recent') }} AS ip
    ON d.sk_patient_id = ip.sk_patient_id
LEFT JOIN {{ ref('fct_person_sus_ae_recent') }} AS ae
    ON d.sk_patient_id = ae.sk_patient_id
WHERE d.age >= 18
    -- Clinical complexity
    AND (
        COALESCE(f.latest_frailty_severity IN ('Moderate', 'Severe'), FALSE)
        OR l.ltc_count >= 3
    )
    -- Utilisation or housebound
    AND (
        ip.apc_nel_12mo >= 2
        OR ae.ae_tot_12mo >= 3
        OR h.is_housebound = TRUE
    )
