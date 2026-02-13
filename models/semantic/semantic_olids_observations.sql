{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Clinical Observations Semantic View
    ==========================================
    
    Semantic model for clinical observations and biomarkers with
    clinically meaningful categories and control status.
    
    Grain: One row per person (latest observation values)
    
    Design Principles:
    - Categories over averages (% at target vs mean value)
    - Patient-specific thresholds where applicable (BP control)
    - Pre-computed clinical classifications from int/fct models
    
    Note: Observation tables only contain rows for people WITH that measurement.
    Some dimensions/facts will be NULL for patients without records.
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id),
    
    bp AS {{ ref('int_blood_pressure_latest') }}
        PRIMARY KEY (person_id),
    
    bp_control AS {{ ref('fct_person_bp_control') }}
        PRIMARY KEY (person_id),
    
    hba1c AS {{ ref('int_hba1c_latest') }}
        PRIMARY KEY (person_id),
    
    cholesterol AS {{ ref('int_cholesterol_latest') }}
        PRIMARY KEY (person_id),
    
    ldl AS {{ ref('int_cholesterol_ldl_latest') }}
        PRIMARY KEY (person_id),
    
    bmi AS {{ ref('int_bmi_latest') }}
        PRIMARY KEY (person_id),
    
    egfr AS {{ ref('int_egfr_latest') }}
        PRIMARY KEY (person_id),
    
    qrisk AS {{ ref('int_qrisk_latest') }}
        PRIMARY KEY (person_id),
    
    acr AS {{ ref('int_urine_acr_latest') }}
        PRIMARY KEY (person_id)
)

RELATIONSHIPS(
    bp (person_id) REFERENCES demographics,
    bp_control (person_id) REFERENCES demographics,
    hba1c (person_id) REFERENCES demographics,
    cholesterol (person_id) REFERENCES demographics,
    ldl (person_id) REFERENCES demographics,
    bmi (person_id) REFERENCES demographics,
    egfr (person_id) REFERENCES demographics,
    qrisk (person_id) REFERENCES demographics,
    acr (person_id) REFERENCES demographics
)

FACTS(
    -- Raw values
    bp.systolic_value,
    bp.diastolic_value,
    hba1c.hba1c_ifcc,
    cholesterol.cholesterol_value,
    ldl.cholesterol_value AS ldl_value,
    bmi.bmi_value,
    egfr.egfr_value,
    qrisk.qrisk_score,
    acr.acr_value,
    bp_control.latest_bp_reading_age_months
)

DIMENSIONS(
    -- Observation Dates
    bp.clinical_effective_date AS bp_date,
    hba1c.clinical_effective_date AS hba1c_date,
    cholesterol.clinical_effective_date AS cholesterol_date,
    ldl.clinical_effective_date AS ldl_date,
    bmi.clinical_effective_date AS bmi_date,
    egfr.clinical_effective_date AS egfr_date,
    qrisk.clinical_effective_date AS qrisk_date,
    acr.clinical_effective_date AS acr_date,
    
    -- Core Demographics
    demographics.gender,
    demographics.age,
    demographics.age_band_5y,
    demographics.age_band_10y,
    demographics.ethnicity_category,
    demographics.is_active,
    
    -- Organisation
    demographics.practice_code,
    demographics.practice_name,
    demographics.pcn_name,
    demographics.borough_registered,
    demographics.neighbourhood_registered,
    
    -- Geography
    demographics.borough_resident,
    demographics.imd_decile_25,
    demographics.imd_quintile_25,
    
    -- Blood Pressure (raw categories)
    bp.is_home_bp_event,
    bp.is_abpm_bp_event,
    bp.is_hypertensive_range,
    
    -- Blood Pressure Control (patient-specific thresholds)
    bp_control.is_overall_bp_controlled,
    bp_control.is_systolic_controlled,
    bp_control.is_diastolic_controlled,
    bp_control.hypertension_stage,
    bp_control.hypertension_stage_number,
    bp_control.applied_patient_group,
    bp_control.is_case_finding_candidate,
    bp_control.is_latest_bp_within_recommended_interval,
    bp_control.recommended_monitoring_interval,
    bp_control.has_t2dm,
    bp_control.has_ckd,
    bp_control.is_diagnosed_htn,
    
    -- HbA1c Categories
    hba1c.hba1c_category,
    hba1c.meets_qof_target,
    hba1c.indicates_diabetes,
    
    -- Cholesterol Categories
    cholesterol.cholesterol_category,
    ldl.LDL_CVD_Target_Met,
    
    -- BMI Categories
    bmi.bmi_category,
    bmi.requires_lower_bmi_thresholds,
    bmi.is_valid_bmi,
    
    -- eGFR / CKD Staging
    egfr.ckd_stage,
    egfr.is_ckd_indicator,
    
    -- QRISK Categories
    qrisk.qrisk_type,
    qrisk.cvd_risk_category,
    qrisk.is_high_cvd_risk,
    qrisk.is_very_high_cvd_risk,
    qrisk.warrants_statin_consideration,
    
    -- Urine ACR Categories
    acr.acr_category,
    acr.is_acr_elevated,
    acr.is_microalbuminuria,
    acr.is_macroalbuminuria
)

METRICS(
    -- Population Counts
    COUNT(DISTINCT demographics.person_id) AS patient_count,
    
    -- Blood Pressure
    COUNT(DISTINCT bp.person_id) AS patients_with_bp,
    COUNT(DISTINCT bp_control.person_id) AS patients_with_bp_control_assessment,
    COUNT(DISTINCT CASE WHEN bp_control.is_overall_bp_controlled THEN demographics.person_id END) AS bp_controlled_count,
    COUNT(DISTINCT CASE WHEN NOT bp_control.is_overall_bp_controlled THEN bp_control.person_id END) AS bp_uncontrolled_count,
    COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number >= 1 THEN demographics.person_id END) AS bp_stage_1_plus_count,
    COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number >= 2 THEN demographics.person_id END) AS bp_stage_2_plus_count,
    COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number = 3 THEN demographics.person_id END) AS bp_stage_3_severe_count,
    COUNT(DISTINCT CASE WHEN bp_control.is_case_finding_candidate THEN demographics.person_id END) AS bp_case_finding_count,
    COUNT(DISTINCT CASE WHEN bp_control.is_latest_bp_within_recommended_interval THEN demographics.person_id END) AS bp_timely_count,
    
    -- HbA1c
    COUNT(DISTINCT hba1c.person_id) AS patients_with_hba1c,
    COUNT(DISTINCT CASE WHEN hba1c.meets_qof_target THEN demographics.person_id END) AS hba1c_at_qof_target_count,
    COUNT(DISTINCT CASE WHEN NOT hba1c.meets_qof_target AND hba1c.person_id IS NOT NULL THEN demographics.person_id END) AS hba1c_above_qof_target_count,
    COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Diabetes - High Risk' THEN demographics.person_id END) AS hba1c_high_risk_count,
    COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Diabetes - Very High Risk' THEN demographics.person_id END) AS hba1c_very_high_risk_count,
    COUNT(DISTINCT CASE WHEN hba1c.hba1c_category IN ('Diabetes - High Risk', 'Diabetes - Very High Risk') THEN demographics.person_id END) AS hba1c_poor_control_count,
    COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Prediabetes' THEN demographics.person_id END) AS hba1c_prediabetes_count,
    COUNT(DISTINCT CASE WHEN hba1c.indicates_diabetes THEN demographics.person_id END) AS hba1c_diabetes_range_count,
    
    -- Cholesterol
    COUNT(DISTINCT cholesterol.person_id) AS patients_with_cholesterol,
    COUNT(DISTINCT CASE WHEN cholesterol.cholesterol_category = 'Desirable' THEN demographics.person_id END) AS cholesterol_desirable_count,
    COUNT(DISTINCT CASE WHEN cholesterol.cholesterol_category = 'High' THEN demographics.person_id END) AS cholesterol_high_count,
    COUNT(DISTINCT ldl.person_id) AS patients_with_ldl,
    COUNT(DISTINCT CASE WHEN ldl.LDL_CVD_Target_Met THEN demographics.person_id END) AS ldl_at_target_count,
    
    -- BMI
    COUNT(DISTINCT bmi.person_id) AS patients_with_bmi,
    COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Normal' THEN demographics.person_id END) AS bmi_normal_count,
    COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Overweight' THEN demographics.person_id END) AS bmi_overweight_count,
    COUNT(DISTINCT CASE WHEN bmi.bmi_category IN ('Obese Class I', 'Obese Class II', 'Obese Class III') THEN demographics.person_id END) AS bmi_obese_count,
    COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Obese Class III' THEN demographics.person_id END) AS bmi_obese_class_3_count,
    COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Underweight' THEN demographics.person_id END) AS bmi_underweight_count,
    
    -- eGFR / CKD
    COUNT(DISTINCT egfr.person_id) AS patients_with_egfr,
    COUNT(DISTINCT CASE WHEN egfr.is_ckd_indicator THEN demographics.person_id END) AS ckd_indicator_count,
    COUNT(DISTINCT CASE WHEN egfr.ckd_stage IN ('3a', '3b', '4', '5') THEN demographics.person_id END) AS ckd_stage_3_plus_count,
    COUNT(DISTINCT CASE WHEN egfr.ckd_stage IN ('4', '5') THEN demographics.person_id END) AS ckd_stage_4_plus_count,
    
    -- QRISK
    COUNT(DISTINCT qrisk.person_id) AS patients_with_qrisk,
    COUNT(DISTINCT CASE WHEN qrisk.is_high_cvd_risk THEN demographics.person_id END) AS qrisk_high_risk_count,
    COUNT(DISTINCT CASE WHEN qrisk.is_very_high_cvd_risk THEN demographics.person_id END) AS qrisk_very_high_risk_count,
    COUNT(DISTINCT CASE WHEN qrisk.warrants_statin_consideration THEN demographics.person_id END) AS qrisk_statin_consideration_count,
    
    -- Urine ACR
    COUNT(DISTINCT acr.person_id) AS patients_with_acr,
    COUNT(DISTINCT CASE WHEN acr.is_acr_elevated THEN demographics.person_id END) AS acr_elevated_count,
    COUNT(DISTINCT CASE WHEN acr.is_microalbuminuria THEN demographics.person_id END) AS acr_microalbuminuria_count,
    COUNT(DISTINCT CASE WHEN acr.is_macroalbuminuria THEN demographics.person_id END) AS acr_macroalbuminuria_count,
    
    -- Average Values (supplementary)
    AVG(bp.systolic_value) AS avg_systolic_bp,
    AVG(bp.diastolic_value) AS avg_diastolic_bp,
    AVG(hba1c.hba1c_ifcc) AS avg_hba1c,
    AVG(cholesterol.cholesterol_value) AS avg_cholesterol,
    AVG(ldl.cholesterol_value) AS avg_ldl,
    AVG(bmi.bmi_value) AS avg_bmi,
    AVG(egfr.egfr_value) AS avg_egfr,
    AVG(qrisk.qrisk_score) AS avg_qrisk,
    AVG(acr.acr_value) AS avg_acr
)

COMMENT = 'OLIDS Clinical Observations Semantic View - Clinical biomarkers with category-based metrics for population health. Raw values available for custom queries. Includes patient-specific BP thresholds.'
