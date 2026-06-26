{{
    config(
        materialized='table',
        tags=['intermediate', 'programme', 'tirzepatide'],
        cluster_by=['person_id'])
}}

/*
Tirzepatide (GLP-1 for obesity) eligibility population.

Composes the eligibility criteria for the NCL/WNL tirzepatide rollout from
existing registers, the two new comorbidity clusters, and ethnicity-adjusted BMI.

Eligibility (both cohorts): >= 4 of 5 qualifying comorbidities
  1. Hypertension                 - fct_person_hypertension_register
  2. Dyslipidaemia                - int_dyslipidaemia_diagnoses_all (DYSLIPIDAEMIA_COD)
  3. Obstructive sleep apnoea     - int_obstructive_sleep_apnoea_diagnoses_all (OBSTRUCTIVE_SLEEP_APNOEA_COD)
  4. Cardiovascular disease       - any of AF / CHD / heart failure / PAD / stroke-TIA registers
                                    (hypertension is excluded here - it counts separately above
                                     to avoid double-counting, per the qualifying-comorbidity list)
  5. Type 2 diabetes mellitus     - fct_person_diabetes_register (diabetes_type = 'Type 2')

BMI band (ethnicity-adjusted via int_bmi_latest, NICE NG246 thresholds):
  - Cohort 1 (Year 1, 2025/26): BMI >= 40   -> Obese Class III
  - Cohort 2 (Year 2, 2026/27): BMI 35-39.9 -> Obese Class II
  The -2.5 adjustment for South Asian, Chinese, other Asian, Middle Eastern,
  Black African and African-Caribbean groups is already baked into bmi_category.

Spine: currently-registered, living adults whose latest valid BMI is in an
eligible band (Class II or III). No eligible person can sit below Class II, so
this narrowing is lossless and keeps the model lean. Comorbidity flags and the
cohort eligibility flags are layered on top. Filter is_eligible_any downstream.
*/

WITH base_population AS (
    -- Currently-registered, living adults (registers don't enforce registration,
    -- so the spine does)
    SELECT
        prac.person_id,
        age.age
    FROM {{ ref('dim_person_current_practice') }} AS prac
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON prac.person_id = age.person_id
    WHERE prac.registration_end_date IS NULL
      AND COALESCE(age.is_deceased, FALSE) = FALSE
      AND age.age >= 18
),

-- BMI gate: latest valid BMI in an eligible band (ethnicity-adjusted)
bmi AS (
    SELECT
        person_id,
        bmi_value AS latest_bmi_value,
        clinical_effective_date AS latest_bmi_date,
        bmi_category,
        requires_lower_bmi_thresholds,
        cardiometabolic_risk_ethnicity_group
    FROM {{ ref('int_bmi_latest') }}
    WHERE bmi_category IN ('Obese Class II', 'Obese Class III')
),

-- Qualifying comorbidity 1: hypertension
hypertension AS (
    SELECT DISTINCT person_id FROM {{ ref('fct_person_hypertension_register') }}
),

-- Qualifying comorbidity 2: dyslipidaemia (any diagnosis observation)
dyslipidaemia AS (
    SELECT DISTINCT person_id FROM {{ ref('int_dyslipidaemia_diagnoses_all') }}
),

-- Qualifying comorbidity 3: obstructive sleep apnoea (any diagnosis observation)
osa AS (
    SELECT DISTINCT person_id FROM {{ ref('int_obstructive_sleep_apnoea_diagnoses_all') }}
),

-- Qualifying comorbidity 4: cardiovascular disease (excludes hypertension)
cvd AS (
    SELECT person_id FROM {{ ref('fct_person_atrial_fibrillation_register') }}
    UNION
    SELECT person_id FROM {{ ref('fct_person_chd_register') }}
    UNION
    SELECT person_id FROM {{ ref('fct_person_heart_failure_register') }}
    UNION
    SELECT person_id FROM {{ ref('fct_person_pad_register') }}
    UNION
    SELECT person_id FROM {{ ref('fct_person_stroke_tia_register') }}
),

-- Qualifying comorbidity 5: type 2 diabetes
type2_diabetes AS (
    SELECT DISTINCT person_id
    FROM {{ ref('fct_person_diabetes_register') }}
    WHERE diabetes_type = 'Type 2'
),

flags AS (
    SELECT
        bp.person_id,
        bp.age,
        bmi.latest_bmi_value,
        bmi.latest_bmi_date,
        bmi.bmi_category,
        bmi.requires_lower_bmi_thresholds,
        bmi.cardiometabolic_risk_ethnicity_group,

        (htn.person_id IS NOT NULL) AS has_hypertension,
        (dys.person_id IS NOT NULL) AS has_dyslipidaemia,
        (osa.person_id IS NOT NULL) AS has_obstructive_sleep_apnoea,
        (cvd.person_id IS NOT NULL) AS has_cardiovascular_disease,
        (t2dm.person_id IS NOT NULL) AS has_type2_diabetes
    FROM base_population AS bp
    INNER JOIN bmi ON bp.person_id = bmi.person_id
    LEFT JOIN hypertension AS htn ON bp.person_id = htn.person_id
    LEFT JOIN dyslipidaemia AS dys ON bp.person_id = dys.person_id
    LEFT JOIN osa ON bp.person_id = osa.person_id
    LEFT JOIN cvd ON bp.person_id = cvd.person_id
    LEFT JOIN type2_diabetes AS t2dm ON bp.person_id = t2dm.person_id
)

SELECT
    person_id,
    age,

    -- BMI
    latest_bmi_value,
    latest_bmi_date,
    bmi_category,
    requires_lower_bmi_thresholds,
    cardiometabolic_risk_ethnicity_group,

    -- Qualifying comorbidities
    has_hypertension,
    has_dyslipidaemia,
    has_obstructive_sleep_apnoea,
    has_cardiovascular_disease,
    has_type2_diabetes,
    (
        has_hypertension::INT
        + has_dyslipidaemia::INT
        + has_obstructive_sleep_apnoea::INT
        + has_cardiovascular_disease::INT
        + has_type2_diabetes::INT
    ) AS qualifying_comorbidity_count,

    -- Cohort BMI bands
    (bmi_category = 'Obese Class III') AS bmi_meets_cohort_1,
    (bmi_category = 'Obese Class II') AS bmi_meets_cohort_2,

    -- Eligibility: >= 4 qualifying comorbidities + cohort BMI band
    (
        (
            has_hypertension::INT
            + has_dyslipidaemia::INT
            + has_obstructive_sleep_apnoea::INT
            + has_cardiovascular_disease::INT
            + has_type2_diabetes::INT
        ) >= 4
        AND bmi_category = 'Obese Class III'
    ) AS is_eligible_cohort_1,
    (
        (
            has_hypertension::INT
            + has_dyslipidaemia::INT
            + has_obstructive_sleep_apnoea::INT
            + has_cardiovascular_disease::INT
            + has_type2_diabetes::INT
        ) >= 4
        AND bmi_category = 'Obese Class II'
    ) AS is_eligible_cohort_2,
    (
        (
            has_hypertension::INT
            + has_dyslipidaemia::INT
            + has_obstructive_sleep_apnoea::INT
            + has_cardiovascular_disease::INT
            + has_type2_diabetes::INT
        ) >= 4
        AND bmi_category IN ('Obese Class II', 'Obese Class III')
    ) AS is_eligible_any
FROM flags
