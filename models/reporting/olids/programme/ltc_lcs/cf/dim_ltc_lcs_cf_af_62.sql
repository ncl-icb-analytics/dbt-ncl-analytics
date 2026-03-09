{{ config(
    materialized='table') }}

-- Intermediate model for LTC LCS Case Finding AF_62: Patients over 65 missing pulse check in last 3 years
-- Uses modular approach: leverages base population, observations intermediate, and exclusions

WITH base_population AS (
    SELECT
        bp.person_id,
        age.age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS bp
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON bp.person_id = age.person_id
    WHERE age.age >= 65
),

pulse_checks AS (
    SELECT
        person_id,
        clinical_effective_date,
        mapped_concept_code,
        mapped_concept_display
    FROM {{ ref('int_ltc_lcs_af_observations') }}
    WHERE
        cluster_id IN ('LCS_PULSE_RATE', 'LCS_PULSE_RHYTHM')
        -- EMIS ICB_CF_AF_62_woEX uses 3-year lookback for pulse checks
        AND clinical_effective_date >= dateadd(YEAR, -3, current_date())
),

pulse_check_summary AS (
    SELECT
        person_id,
        max(clinical_effective_date) AS latest_pulse_check_date,
        boolor_agg(TRUE) AS has_pulse_check,
        array_agg(DISTINCT mapped_concept_code) AS all_pulse_check_codes,
        array_agg(DISTINCT mapped_concept_display) AS all_pulse_check_displays
    FROM pulse_checks
    GROUP BY person_id
),

health_checks AS (
    SELECT
        person_id,
        max(clinical_effective_date) AS latest_health_check_date
    FROM {{ ref('int_nhs_health_check_latest') }}
    GROUP BY person_id
),

exclusions AS (
    SELECT
        person_id,
        has_excluding_condition
    FROM {{ ref('int_ltc_lcs_cf_exclusions') }}
),

-- EMIS ICB_CF_AF_62_BASE excludes patients already in AF_61 (medication case finding)
af_61_patients AS (
    SELECT DISTINCT person_id
    FROM {{ ref('dim_ltc_lcs_cf_af_61') }}
)

SELECT
    bp.person_id,
    bp.age,
    pcs.latest_pulse_check_date,
    hc.latest_health_check_date,
    ex.has_excluding_condition,
    pcs.all_pulse_check_codes,
    pcs.all_pulse_check_displays,
    coalesce(pcs.has_pulse_check, FALSE) AS has_pulse_check
FROM base_population AS bp
LEFT JOIN pulse_check_summary AS pcs ON bp.person_id = pcs.person_id
LEFT JOIN health_checks AS hc ON bp.person_id = hc.person_id
LEFT JOIN exclusions AS ex ON bp.person_id = ex.person_id
WHERE coalesce(pcs.has_pulse_check, FALSE) = FALSE
    -- Mutual exclusivity: exclude patients already in AF_61 medication case finding
    AND bp.person_id NOT IN (SELECT person_id FROM af_61_patients)
