{{ config(
    materialized='table') }}

-- Intermediate model for HF_61 case-finding base population
-- Current registered population excluding metabolic LTC, AF register, HF register, and recorded HF evidence

WITH current_registered_population AS (
    SELECT DISTINCT
        prac.person_id,
        age.age
    FROM {{ ref('dim_person_current_practice') }} AS prac
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON prac.person_id = age.person_id
    WHERE prac.registration_end_date IS NULL
),

metabolic_ltc AS (
    SELECT
        person_id
    FROM {{ ref('int_ltc_lcs_cf_exclusions') }}
    WHERE has_type2_diabetes = TRUE
        OR has_hyperlipidaemia = TRUE
        OR has_nafld = TRUE
),

af_register AS (
    SELECT DISTINCT person_id
    FROM {{ ref('fct_person_atrial_fibrillation_register') }}
    WHERE is_on_register = TRUE
),

hf_register AS (
    SELECT DISTINCT person_id
    FROM {{ ref('fct_person_heart_failure_register') }}
    WHERE is_on_register = TRUE
),

hf_evidence AS (
    SELECT DISTINCT
        person_id,
        clinical_effective_date AS latest_hf_evidence_date
    FROM {{ ref('int_ltc_lcs_hf_observations') }}
    WHERE valueset_friendly_name = 'eligible_for_hf_casefinding_vs1'
),

base_population AS (
    SELECT
        pop.person_id,
        pop.age,
        (met.person_id IS NOT NULL) AS is_in_ics_metabolic_ltc,
        (af.person_id IS NOT NULL) AS is_on_af_register,
        (hf.person_id IS NOT NULL) AS is_on_hf_register,
        (obs.person_id IS NOT NULL) AS has_recorded_hf_evidence,
        obs.latest_hf_evidence_date
    FROM current_registered_population AS pop
    LEFT JOIN metabolic_ltc AS met
        ON pop.person_id = met.person_id
    LEFT JOIN af_register AS af
        ON pop.person_id = af.person_id
    LEFT JOIN hf_register AS hf
        ON pop.person_id = hf.person_id
    LEFT JOIN hf_evidence AS obs
        ON pop.person_id = obs.person_id
)

SELECT
    person_id,
    age,
    latest_hf_evidence_date,
    is_in_ics_metabolic_ltc,
    is_on_af_register,
    is_on_hf_register,
    has_recorded_hf_evidence
FROM base_population
WHERE is_in_ics_metabolic_ltc = FALSE
    AND is_on_af_register = FALSE
    AND is_on_hf_register = FALSE
    AND has_recorded_hf_evidence = FALSE
