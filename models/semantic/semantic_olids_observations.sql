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
        PRIMARY KEY (person_id)
        COMMENT = 'Base population - all persons with registration history',
    
    bp AS {{ ref('int_blood_pressure_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest blood pressure measurement (systolic/diastolic)',
    
    bp_control AS {{ ref('fct_person_bp_control') }}
        PRIMARY KEY (person_id)
        COMMENT = 'BP control status with patient-specific thresholds (T2DM, CKD, age-adjusted)',
    
    hba1c AS {{ ref('int_hba1c_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest HbA1c with NICE-aligned clinical categories',
    
    cholesterol AS {{ ref('int_cholesterol_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest total cholesterol measurement',
    
    ldl AS {{ ref('int_cholesterol_ldl_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest LDL cholesterol measurement',
    
    bmi AS {{ ref('int_bmi_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest BMI with ethnicity-adjusted categories (NICE NG246)',
    
    egfr AS {{ ref('int_egfr_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest eGFR with CKD staging',
    
    qrisk AS {{ ref('int_qrisk_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest QRISK cardiovascular risk score',
    
    acr AS {{ ref('int_urine_acr_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest urine albumin:creatinine ratio'
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
    -- Raw values (for reference, not primary analysis)
    bp.systolic_value COMMENT = 'Systolic BP (mmHg)',
    bp.diastolic_value COMMENT = 'Diastolic BP (mmHg)',
    hba1c.hba1c_ifcc COMMENT = 'HbA1c value (mmol/mol IFCC)',
    cholesterol.cholesterol_value COMMENT = 'Total cholesterol (mmol/L)',
    ldl.cholesterol_value COMMENT = 'LDL cholesterol (mmol/L)',
    bmi.bmi_value COMMENT = 'BMI value (kg/m²)',
    egfr.egfr_value COMMENT = 'eGFR value (mL/min/1.73m²)',
    qrisk.qrisk_score COMMENT = 'QRISK score (%)',
    acr.acr_value COMMENT = 'Urine ACR (mg/mmol)',
    
    -- BP control timeliness
    bp_control.latest_bp_reading_age_months COMMENT = 'Months since last BP reading'
)

DIMENSIONS(
    -- ===================
    -- OBSERVATION DATES (for timeliness queries)
    -- ===================
    bp.clinical_effective_date AS bp_date COMMENT = 'Date of latest BP reading',
    hba1c.clinical_effective_date AS hba1c_date COMMENT = 'Date of latest HbA1c',
    cholesterol.clinical_effective_date AS cholesterol_date COMMENT = 'Date of latest cholesterol',
    ldl.clinical_effective_date AS ldl_date COMMENT = 'Date of latest LDL',
    bmi.clinical_effective_date AS bmi_date COMMENT = 'Date of latest BMI',
    egfr.clinical_effective_date AS egfr_date COMMENT = 'Date of latest eGFR',
    qrisk.clinical_effective_date AS qrisk_date COMMENT = 'Date of latest QRISK',
    acr.clinical_effective_date AS acr_date COMMENT = 'Date of latest ACR',
    
    -- Core Demographics (for segmentation)
    demographics.gender COMMENT = 'Patient gender',
    demographics.age COMMENT = 'Current age in years',
    demographics.age_band_5y COMMENT = '5-year age bands',
    demographics.age_band_10y COMMENT = '10-year age bands',
    demographics.ethnicity_category COMMENT = 'Ethnicity category',
    demographics.is_active COMMENT = 'Currently registered',
    
    -- Organisation
    demographics.practice_code COMMENT = 'GP practice code',
    demographics.practice_name COMMENT = 'GP practice name',
    demographics.pcn_name COMMENT = 'Primary Care Network',
    demographics.borough_registered COMMENT = 'Registration borough',
    demographics.neighbourhood_registered COMMENT = 'Registration neighbourhood',
    
    -- Geography
    demographics.borough_resident COMMENT = 'Residence borough',
    demographics.imd_decile_25 COMMENT = 'IMD 2025 decile',
    demographics.imd_quintile_25 COMMENT = 'IMD 2025 quintile',
    
    -- Blood Pressure (raw categories)
    bp.is_home_bp_event COMMENT = 'Home BP measurement',
    bp.is_abpm_bp_event COMMENT = 'Ambulatory BP measurement',
    bp.is_hypertensive_range COMMENT = 'BP in hypertensive range (≥140/90 or ≥135/85 ABPM)',
    
    -- Blood Pressure Control (patient-specific thresholds)
    bp_control.is_overall_bp_controlled COMMENT = 'BP controlled (both systolic AND diastolic at patient-specific target)',
    bp_control.is_systolic_controlled COMMENT = 'Systolic BP at target',
    bp_control.is_diastolic_controlled COMMENT = 'Diastolic BP at target',
    bp_control.hypertension_stage COMMENT = 'Hypertension stage (Normal, Stage 1, Stage 2, Stage 3 Severe)',
    bp_control.hypertension_stage_number COMMENT = 'Hypertension stage number (0-3)',
    bp_control.applied_patient_group COMMENT = 'Threshold group applied (T2DM, CKD, CKD_ACR_GE_70, AGE_GE_80, AGE_LT_80)',
    bp_control.is_case_finding_candidate COMMENT = 'Elevated BP but not on HTN register - case finding opportunity',
    bp_control.is_latest_bp_within_recommended_interval COMMENT = 'BP reading within recommended monitoring interval',
    bp_control.recommended_monitoring_interval COMMENT = 'Recommended BP monitoring interval',
    bp_control.has_t2dm COMMENT = 'Has Type 2 diabetes (affects BP threshold)',
    bp_control.has_ckd COMMENT = 'Has CKD (affects BP threshold)',
    bp_control.is_diagnosed_htn COMMENT = 'On hypertension register',
    
    -- HbA1c Categories (NICE-aligned)
    hba1c.hba1c_category COMMENT = 'HbA1c category (Normal, Prediabetes, At NICE Target, Acceptable, Above Target, High Risk, Very High Risk)',
    hba1c.meets_qof_target COMMENT = 'HbA1c ≤58 mmol/mol (QOF target)',
    hba1c.indicates_diabetes COMMENT = 'HbA1c ≥48 mmol/mol (diabetes diagnostic)',
    
    -- Cholesterol Categories
    cholesterol.cholesterol_category COMMENT = 'Cholesterol category (Desirable/Borderline/High)',
    
    -- LDL Cholesterol
    ldl.LDL_CVD_Target_Met COMMENT = 'LDL at CVD target',
    
    -- BMI Categories (ethnicity-adjusted per NICE NG246)
    bmi.bmi_category COMMENT = 'BMI category (Underweight, Normal, Overweight, Obese Class I/II/III)',
    bmi.requires_lower_bmi_thresholds COMMENT = 'Uses lower BMI thresholds for cardiometabolic risk',
    bmi.is_valid_bmi COMMENT = 'BMI in valid range',
    
    -- eGFR / CKD Staging
    egfr.ckd_stage COMMENT = 'CKD stage (1-5)',
    egfr.is_ckd_indicator COMMENT = 'eGFR indicates CKD',
    
    -- QRISK Categories
    qrisk.qrisk_type COMMENT = 'QRISK version (QRISK2/QRISK3)',
    qrisk.cvd_risk_category COMMENT = 'CVD risk category',
    qrisk.is_high_cvd_risk COMMENT = 'QRISK ≥10% (high CVD risk)',
    qrisk.is_very_high_cvd_risk COMMENT = 'QRISK ≥20% (very high CVD risk)',
    qrisk.warrants_statin_consideration COMMENT = 'QRISK warrants statin consideration',
    
    -- Urine ACR Categories
    acr.acr_category COMMENT = 'ACR category (Normal/Moderate/Severe)',
    acr.is_acr_elevated COMMENT = 'ACR ≥3 mg/mmol',
    acr.is_microalbuminuria COMMENT = 'Microalbuminuria present',
    acr.is_macroalbuminuria COMMENT = 'Macroalbuminuria present'
)

METRICS(
    -- Population Counts
    COUNT(DISTINCT demographics.person_id) AS patient_count
        COMMENT = 'Total patients',
    
    -- ===================
    -- BLOOD PRESSURE
    -- ===================
    COUNT(DISTINCT bp.person_id) AS patients_with_bp
        COMMENT = 'Patients with BP measurement',
    
    COUNT(DISTINCT bp_control.person_id) AS patients_with_bp_control_assessment
        COMMENT = 'Patients with BP control assessment',
    
    -- Control metrics (patient-specific thresholds)
    COUNT(DISTINCT CASE WHEN bp_control.is_overall_bp_controlled THEN demographics.person_id END) AS bp_controlled_count
        COMMENT = 'Patients with BP controlled (patient-specific threshold)',
    
    COUNT(DISTINCT CASE WHEN NOT bp_control.is_overall_bp_controlled THEN bp_control.person_id END) AS bp_uncontrolled_count
        COMMENT = 'Patients with BP NOT controlled',
    
    -- Hypertension staging
    COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number >= 1 THEN demographics.person_id END) AS bp_stage_1_plus_count
        COMMENT = 'Patients with Stage 1+ hypertension',
    
    COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number >= 2 THEN demographics.person_id END) AS bp_stage_2_plus_count
        COMMENT = 'Patients with Stage 2+ hypertension',
    
    COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number = 3 THEN demographics.person_id END) AS bp_stage_3_severe_count
        COMMENT = 'Patients with Stage 3 (severe) hypertension',
    
    -- Case finding
    COUNT(DISTINCT CASE WHEN bp_control.is_case_finding_candidate THEN demographics.person_id END) AS bp_case_finding_count
        COMMENT = 'Case finding candidates (elevated BP, not on HTN register)',
    
    -- Timeliness
    COUNT(DISTINCT CASE WHEN bp_control.is_latest_bp_within_recommended_interval THEN demographics.person_id END) AS bp_timely_count
        COMMENT = 'Patients with BP within recommended monitoring interval',
    
    -- ===================
    -- HbA1c
    -- ===================
    COUNT(DISTINCT hba1c.person_id) AS patients_with_hba1c
        COMMENT = 'Patients with HbA1c measurement',
    
    -- QOF target
    COUNT(DISTINCT CASE WHEN hba1c.meets_qof_target THEN demographics.person_id END) AS hba1c_at_qof_target_count
        COMMENT = 'Patients with HbA1c ≤58 (QOF controlled)',
    
    COUNT(DISTINCT CASE WHEN NOT hba1c.meets_qof_target AND hba1c.person_id IS NOT NULL THEN demographics.person_id END) AS hba1c_above_qof_target_count
        COMMENT = 'Patients with HbA1c >58 (above QOF target)',
    
    -- Risk stratification
    COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Diabetes - High Risk' THEN demographics.person_id END) AS hba1c_high_risk_count
        COMMENT = 'Patients with HbA1c 75-85 (high risk)',
    
    COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Diabetes - Very High Risk' THEN demographics.person_id END) AS hba1c_very_high_risk_count
        COMMENT = 'Patients with HbA1c ≥86 (very high risk)',
    
    COUNT(DISTINCT CASE WHEN hba1c.hba1c_category IN ('Diabetes - High Risk', 'Diabetes - Very High Risk') THEN demographics.person_id END) AS hba1c_poor_control_count
        COMMENT = 'Patients with HbA1c ≥75 (poor control)',
    
    -- Prediabetes
    COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Prediabetes' THEN demographics.person_id END) AS hba1c_prediabetes_count
        COMMENT = 'Patients with HbA1c 42-47 (prediabetes range)',
    
    -- Diabetes diagnostic
    COUNT(DISTINCT CASE WHEN hba1c.indicates_diabetes THEN demographics.person_id END) AS hba1c_diabetes_range_count
        COMMENT = 'Patients with HbA1c ≥48 (diabetes range)',
    
    -- ===================
    -- CHOLESTEROL
    -- ===================
    COUNT(DISTINCT cholesterol.person_id) AS patients_with_cholesterol
        COMMENT = 'Patients with cholesterol measurement',
    
    COUNT(DISTINCT CASE WHEN cholesterol.cholesterol_category = 'Desirable' THEN demographics.person_id END) AS cholesterol_desirable_count
        COMMENT = 'Patients with desirable cholesterol (<5 mmol/L)',
    
    COUNT(DISTINCT CASE WHEN cholesterol.cholesterol_category = 'High' THEN demographics.person_id END) AS cholesterol_high_count
        COMMENT = 'Patients with high cholesterol (≥6.2 mmol/L)',
    
    -- LDL
    COUNT(DISTINCT ldl.person_id) AS patients_with_ldl
        COMMENT = 'Patients with LDL measurement',
    
    COUNT(DISTINCT CASE WHEN ldl.LDL_CVD_Target_Met THEN demographics.person_id END) AS ldl_at_target_count
        COMMENT = 'Patients with LDL at CVD target',
    
    -- ===================
    -- BMI
    -- ===================
    COUNT(DISTINCT bmi.person_id) AS patients_with_bmi
        COMMENT = 'Patients with BMI measurement',
    
    COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Normal' THEN demographics.person_id END) AS bmi_normal_count
        COMMENT = 'Patients with normal BMI',
    
    COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Overweight' THEN demographics.person_id END) AS bmi_overweight_count
        COMMENT = 'Patients who are overweight',
    
    COUNT(DISTINCT CASE WHEN bmi.bmi_category IN ('Obese Class I', 'Obese Class II', 'Obese Class III') THEN demographics.person_id END) AS bmi_obese_count
        COMMENT = 'Patients with obesity (any class)',
    
    COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Obese Class III' THEN demographics.person_id END) AS bmi_obese_class_3_count
        COMMENT = 'Patients with severe obesity (Class III)',
    
    COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Underweight' THEN demographics.person_id END) AS bmi_underweight_count
        COMMENT = 'Patients who are underweight',
    
    -- ===================
    -- eGFR / CKD
    -- ===================
    COUNT(DISTINCT egfr.person_id) AS patients_with_egfr
        COMMENT = 'Patients with eGFR measurement',
    
    COUNT(DISTINCT CASE WHEN egfr.is_ckd_indicator THEN demographics.person_id END) AS ckd_indicator_count
        COMMENT = 'Patients with eGFR indicating CKD',
    
    COUNT(DISTINCT CASE WHEN egfr.ckd_stage IN ('3a', '3b', '4', '5') THEN demographics.person_id END) AS ckd_stage_3_plus_count
        COMMENT = 'Patients with CKD Stage 3+',
    
    COUNT(DISTINCT CASE WHEN egfr.ckd_stage IN ('4', '5') THEN demographics.person_id END) AS ckd_stage_4_plus_count
        COMMENT = 'Patients with CKD Stage 4-5',
    
    -- ===================
    -- QRISK
    -- ===================
    COUNT(DISTINCT qrisk.person_id) AS patients_with_qrisk
        COMMENT = 'Patients with QRISK score',
    
    COUNT(DISTINCT CASE WHEN qrisk.is_high_cvd_risk THEN demographics.person_id END) AS qrisk_high_risk_count
        COMMENT = 'Patients with QRISK ≥10% (high CVD risk)',
    
    COUNT(DISTINCT CASE WHEN qrisk.is_very_high_cvd_risk THEN demographics.person_id END) AS qrisk_very_high_risk_count
        COMMENT = 'Patients with QRISK ≥20% (very high CVD risk)',
    
    COUNT(DISTINCT CASE WHEN qrisk.warrants_statin_consideration THEN demographics.person_id END) AS qrisk_statin_consideration_count
        COMMENT = 'Patients where QRISK warrants statin consideration',
    
    -- ===================
    -- URINE ACR
    -- ===================
    COUNT(DISTINCT acr.person_id) AS patients_with_acr
        COMMENT = 'Patients with urine ACR measurement',
    
    COUNT(DISTINCT CASE WHEN acr.is_acr_elevated THEN demographics.person_id END) AS acr_elevated_count
        COMMENT = 'Patients with elevated ACR (≥3 mg/mmol)',
    
    COUNT(DISTINCT CASE WHEN acr.is_microalbuminuria THEN demographics.person_id END) AS acr_microalbuminuria_count
        COMMENT = 'Patients with microalbuminuria',
    
    COUNT(DISTINCT CASE WHEN acr.is_macroalbuminuria THEN demographics.person_id END) AS acr_macroalbuminuria_count
        COMMENT = 'Patients with macroalbuminuria',
    
    -- ===================
    -- AVERAGE VALUES (supplementary - use categories for population health)
    -- ===================
    AVG(bp.systolic_value) AS avg_systolic_bp
        COMMENT = 'Average systolic BP (mmHg) - use bp_controlled_count for population metrics',
    
    AVG(bp.diastolic_value) AS avg_diastolic_bp
        COMMENT = 'Average diastolic BP (mmHg) - use bp_controlled_count for population metrics',
    
    AVG(hba1c.hba1c_ifcc) AS avg_hba1c
        COMMENT = 'Average HbA1c (mmol/mol) - use hba1c_at_qof_target_count for population metrics',
    
    AVG(cholesterol.cholesterol_value) AS avg_cholesterol
        COMMENT = 'Average total cholesterol (mmol/L)',
    
    AVG(ldl.cholesterol_value) AS avg_ldl
        COMMENT = 'Average LDL cholesterol (mmol/L)',
    
    AVG(bmi.bmi_value) AS avg_bmi
        COMMENT = 'Average BMI (kg/m²) - use bmi category counts for population metrics',
    
    AVG(egfr.egfr_value) AS avg_egfr
        COMMENT = 'Average eGFR (mL/min/1.73m²)',
    
    AVG(qrisk.qrisk_score) AS avg_qrisk
        COMMENT = 'Average QRISK score (%) - use qrisk_high_risk_count for population metrics',
    
    AVG(acr.acr_value) AS avg_acr
        COMMENT = 'Average urine ACR (mg/mmol)'
)

COMMENT = 'OLIDS Clinical Observations Semantic View - Clinical biomarkers with category-based metrics for population health. Raw values available in FACTS for custom queries. Includes patient-specific BP thresholds.'
