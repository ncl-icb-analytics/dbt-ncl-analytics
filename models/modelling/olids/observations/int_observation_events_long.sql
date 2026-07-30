{{
    config(
        materialized='table',
        cluster_by=['person_id', 'observation_type'])
}}

/*
Long-format biomarker observation history — one row per observation event.

Unions the per-observation "_all" biomarker models into a single tall table
(person_id, observation_type, clinical_effective_date, value, unit, category)
so that serial / latest-N / trajectory questions can be answered with window
functions over a single grain. The latest-only models (int_*_latest) and the
sem_olids_observations view answer "current value" questions; this model and
sem_olids_observations_history answer "over time" questions.

Why long, not wide: the source _all models are many-rows-per-person with
divergent schemas. Co-joining them on person_id would fan out (cartesian).
Stacking them long keeps a single, safe grain and a uniform shape, at the
cost of a generic VARCHAR category per type.

Blood pressure emits two rows per reading (Systolic BP, Diastolic BP).

Count-style markers (haemoglobin, platelets, eosinophils, ALT, GGT, bilirubin) are filtered
to a non-null inferred value, non-negative and not extreme outliers, matching their _latest
models. Typed markers are included where the value is non-null.
*/

WITH events AS (

    -- Cardiovascular: Blood Pressure (one row each for systolic and diastolic)
    SELECT person_id, systolic_observation_id AS source_observation_id,
        'Systolic BP' AS observation_type, 'Cardiovascular' AS observation_group,
        clinical_effective_date, systolic_value::FLOAT AS value, 'mmHg' AS unit,
        (CASE WHEN is_hypertensive_range THEN 'Hypertensive range' ELSE 'Below hypertensive range' END)::VARCHAR AS category
    FROM {{ ref('int_blood_pressure_all') }}
    WHERE systolic_value IS NOT NULL

    UNION ALL
    SELECT person_id, diastolic_observation_id AS source_observation_id,
        'Diastolic BP', 'Cardiovascular',
        clinical_effective_date, diastolic_value::FLOAT, 'mmHg',
        NULL::VARCHAR
    FROM {{ ref('int_blood_pressure_all') }}
    WHERE diastolic_value IS NOT NULL

    -- Cardiovascular: Cholesterol, LDL, QRISK
    UNION ALL
    SELECT person_id, id, 'Total Cholesterol', 'Cardiovascular',
        clinical_effective_date, cholesterol_value::FLOAT, 'mmol/L', cholesterol_category::VARCHAR
    FROM {{ ref('int_cholesterol_all') }}
    WHERE cholesterol_value IS NOT NULL

    UNION ALL
    SELECT person_id, id, 'LDL Cholesterol', 'Cardiovascular',
        clinical_effective_date, cholesterol_value::FLOAT, 'mmol/L', LDL_CVD_Target_Met::VARCHAR
    FROM {{ ref('int_cholesterol_ldl_all') }}
    WHERE cholesterol_value IS NOT NULL

    UNION ALL
    SELECT person_id, id, 'QRISK', 'Cardiovascular',
        clinical_effective_date, qrisk_score::FLOAT, '%', cvd_risk_category::VARCHAR
    FROM {{ ref('int_qrisk_all') }}
    WHERE qrisk_score IS NOT NULL

    -- Metabolic: HbA1c, BMI, Waist circumference
    UNION ALL
    SELECT person_id, id, 'HbA1c', 'Metabolic',
        clinical_effective_date, hba1c_ifcc::FLOAT, 'mmol/mol', hba1c_category::VARCHAR
    FROM {{ ref('int_hba1c_all') }}
    WHERE hba1c_ifcc IS NOT NULL

    UNION ALL
    SELECT person_id, id, 'BMI', 'Metabolic',
        clinical_effective_date, bmi_value::FLOAT, 'kg/m2', bmi_category::VARCHAR
    FROM {{ ref('int_bmi_all') }}
    WHERE bmi_value IS NOT NULL

    UNION ALL
    SELECT person_id, id, 'Waist Circumference', 'Metabolic',
        clinical_effective_date, waist_circumference_value::FLOAT, 'cm', waist_risk_category::VARCHAR
    FROM {{ ref('int_waist_circumference_all') }}
    WHERE waist_circumference_value IS NOT NULL

    -- Renal: eGFR, Creatinine, Urine ACR
    UNION ALL
    SELECT person_id, id, 'eGFR', 'Renal',
        clinical_effective_date, egfr_value::FLOAT, 'mL/min/1.73m2', ckd_stage::VARCHAR
    FROM {{ ref('int_egfr_all') }}
    WHERE egfr_value IS NOT NULL

    UNION ALL
    SELECT person_id, id, 'Creatinine', 'Renal',
        clinical_effective_date, creatinine_value::FLOAT, 'umol/L', creatinine_category::VARCHAR
    FROM {{ ref('int_creatinine_all') }}
    WHERE creatinine_value IS NOT NULL

    UNION ALL
    SELECT person_id, id, 'Urine ACR', 'Renal',
        clinical_effective_date, acr_value::FLOAT, 'mg/mmol', acr_category::VARCHAR
    FROM {{ ref('int_urine_acr_all') }}
    WHERE acr_value IS NOT NULL

    -- Liver: ALT, GGT, Bilirubin
    UNION ALL
    SELECT person_id, id, 'ALT', 'Liver',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, alt_category::VARCHAR
    FROM {{ ref('int_alt_all') }}
    WHERE NOT is_negative AND NOT is_extreme_outlier AND inferred_value IS NOT NULL

    UNION ALL
    SELECT person_id, id, 'GGT', 'Liver',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, ggt_category::VARCHAR
    FROM {{ ref('int_ggt_all') }}
    WHERE NOT is_negative AND NOT is_extreme_outlier AND inferred_value IS NOT NULL

    UNION ALL
    SELECT person_id, id, 'Bilirubin', 'Liver',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, bilirubin_category::VARCHAR
    FROM {{ ref('int_bilirubin_all') }}
    WHERE NOT is_negative AND NOT is_extreme_outlier AND inferred_value IS NOT NULL

    -- Haematology: Haemoglobin, Platelets, Eosinophils
    UNION ALL
    SELECT person_id, id, 'Haemoglobin', 'Haematology',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, haemoglobin_category::VARCHAR
    FROM {{ ref('int_haemoglobin_all') }}
    WHERE NOT is_negative AND NOT is_extreme_outlier AND inferred_value IS NOT NULL

    UNION ALL
    SELECT person_id, id, 'Platelets', 'Haematology',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, platelets_category::VARCHAR
    FROM {{ ref('int_platelets_all') }}
    WHERE NOT is_negative AND NOT is_extreme_outlier AND inferred_value IS NOT NULL

    UNION ALL
    SELECT person_id, id, 'Eosinophils', 'Haematology',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, eosinophil_category::VARCHAR
    FROM {{ ref('int_eosinophil_count') }}
    WHERE NOT is_negative AND NOT is_extreme_outlier AND inferred_value IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'person_id', 'observation_type', 'clinical_effective_date',
        'source_observation_id', 'value'
    ]) }} AS observation_event_id,
    person_id,
    observation_type,
    observation_group,
    clinical_effective_date,
    value,
    unit,
    category,
    source_observation_id
FROM events
-- Date sanity: a reading cannot post-date today. Legacy pre-1990 dates are
-- kept (transferred records) — window filters should state their range.
WHERE clinical_effective_date <= CURRENT_DATE
