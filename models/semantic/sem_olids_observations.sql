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
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Base population - all persons with registration history',
    
    bp AS {{ ref('int_blood_pressure_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest blood pressure measurement',
    
    bp_control AS {{ ref('fct_person_bp_control') }}
        PRIMARY KEY (person_id)
        COMMENT = 'BP control status with patient-specific thresholds based on T2DM, CKD, age',
    
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
        COMMENT = 'Latest BMI with ethnicity-adjusted categories per NICE NG246',
    
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
    bp.systolic_value AS systolic_value COMMENT = 'Systolic BP (mmHg)',
    bp.diastolic_value AS diastolic_value COMMENT = 'Diastolic BP (mmHg)',
    hba1c.hba1c_ifcc AS hba1c_ifcc COMMENT = 'HbA1c value (mmol/mol IFCC)',
    cholesterol.cholesterol_value AS cholesterol_value COMMENT = 'Total cholesterol (mmol/L)',
    ldl.cholesterol_value AS cholesterol_value COMMENT = 'LDL cholesterol (mmol/L)',
    bmi.bmi_value AS bmi_value COMMENT = 'BMI value (kg/m2)',
    egfr.egfr_value AS egfr_value COMMENT = 'eGFR value (mL/min/1.73m2)',
    qrisk.qrisk_score AS qrisk_score WITH SYNONYMS = ('CVD risk', 'cardiovascular risk') COMMENT = 'QRISK score (%)',
    acr.acr_value AS acr_value COMMENT = 'Urine ACR (mg/mmol)',
    bp_control.latest_bp_reading_age_months AS latest_bp_reading_age_months COMMENT = 'Months since last BP reading'
)

DIMENSIONS(
    -- Observation Dates (each table has clinical_effective_date, accessible as table.clinical_effective_date)
    bp.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest BP reading',
    hba1c.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest HbA1c',
    cholesterol.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest cholesterol',
    ldl.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest LDL',
    bmi.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest BMI',
    egfr.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest eGFR',
    qrisk.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest QRISK',
    acr.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest ACR',
    
    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender',
    demographics.age AS age COMMENT = 'Current age in years',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category',
    demographics.is_active AS is_active COMMENT = 'Currently registered',
    
    -- Organisation
    demographics.practice_code AS practice_code COMMENT = 'GP practice code',
    demographics.practice_name AS practice_name COMMENT = 'GP practice name',
    demographics.pcn_name AS pcn_name COMMENT = 'Primary Care Network',
    demographics.borough_registered AS borough_registered COMMENT = 'Registration borough',
    demographics.neighbourhood_registered AS neighbourhood_registered COMMENT = 'Registration neighbourhood',
    
    -- Geography
    demographics.borough_resident AS borough_resident COMMENT = 'Residence borough',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived)',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile',
    
    -- Blood Pressure (raw)
    bp.is_home_bp_event AS is_home_bp_event WITH SYNONYMS = ('HBPM', 'home monitoring', 'home BP') COMMENT = 'Home BP measurement',
    bp.is_abpm_bp_event AS is_abpm_bp_event WITH SYNONYMS = ('ABPM', 'ambulatory', '24-hour') COMMENT = 'Ambulatory BP measurement',
    bp.is_hypertensive_range AS is_hypertensive_range COMMENT = 'BP in hypertensive range',
    
    -- Blood Pressure Control
    bp_control.is_overall_bp_controlled AS is_overall_bp_controlled WITH SYNONYMS = ('BP at target', 'BP controlled', 'controlled') COMMENT = 'BP controlled (patient-specific threshold)',
    bp_control.is_systolic_controlled AS is_systolic_controlled COMMENT = 'Systolic BP at target',
    bp_control.is_diastolic_controlled AS is_diastolic_controlled COMMENT = 'Diastolic BP at target',
    bp_control.hypertension_stage AS hypertension_stage COMMENT = 'Hypertension stage (Normal, Stage 1, 2, 3 Severe)',
    bp_control.hypertension_stage_number AS hypertension_stage_number COMMENT = 'Hypertension stage number (0-3)',
    bp_control.applied_patient_group AS applied_patient_group WITH SYNONYMS = ('BP threshold group') COMMENT = 'Which threshold applied (T2DM, CKD, AGE_GE_80, etc.)',
    bp_control.is_case_finding_candidate AS is_case_finding_candidate WITH SYNONYMS = ('BP case finding') COMMENT = 'Elevated BP but not on HTN register',
    bp_control.is_latest_bp_within_recommended_interval AS is_latest_bp_within_recommended_interval WITH SYNONYMS = ('BP timely', 'timely BP') COMMENT = 'BP within recommended interval',
    bp_control.has_t2dm AS has_t2dm COMMENT = 'Has Type 2 diabetes (affects BP threshold)',
    bp_control.has_ckd AS has_ckd COMMENT = 'Has CKD (affects BP threshold)',
    bp_control.is_diagnosed_htn AS is_diagnosed_htn WITH SYNONYMS = ('on HTN register', 'diagnosed hypertension') COMMENT = 'On hypertension register',
    
    -- HbA1c Categories
    hba1c.hba1c_category AS hba1c_category COMMENT = 'HbA1c category (Normal, Prediabetes, At NICE Target, Acceptable, Above Target, High Risk, Very High Risk)',
    hba1c.meets_qof_target AS meets_qof_target WITH SYNONYMS = ('HbA1c controlled', 'at target') COMMENT = 'HbA1c <=58 mmol/mol (QOF target)',
    hba1c.indicates_diabetes AS indicates_diabetes COMMENT = 'HbA1c >=48 mmol/mol (diabetes diagnostic)',
    
    -- Cholesterol Categories
    cholesterol.cholesterol_category AS cholesterol_category COMMENT = 'Cholesterol category (Desirable/Borderline/High)',
    ldl.LDL_CVD_Target_Met AS LDL_CVD_Target_Met COMMENT = 'LDL at CVD target',
    
    -- BMI Categories
    bmi.bmi_category AS bmi_category COMMENT = 'BMI category (Underweight, Normal, Overweight, Obese Class I/II/III)',
    bmi.requires_lower_bmi_thresholds AS requires_lower_bmi_thresholds COMMENT = 'Uses lower BMI thresholds for ethnicity',
    bmi.is_valid_bmi AS is_valid_bmi COMMENT = 'BMI in valid range',
    
    -- eGFR / CKD Staging
    egfr.ckd_stage AS ckd_stage COMMENT = 'CKD stage (1, 2, 3a, 3b, 4, 5)',
    egfr.is_ckd_indicator AS is_ckd_indicator COMMENT = 'eGFR indicates CKD',
    
    -- QRISK Categories
    qrisk.qrisk_type AS qrisk_type COMMENT = 'QRISK version (QRISK2/QRISK3)',
    qrisk.cvd_risk_category AS cvd_risk_category COMMENT = 'CVD risk category',
    qrisk.is_high_cvd_risk AS is_high_cvd_risk WITH SYNONYMS = ('high risk', 'QRISK >= 10') COMMENT = 'QRISK >=10% (high CVD risk)',
    qrisk.is_very_high_cvd_risk AS is_very_high_cvd_risk COMMENT = 'QRISK >=20% (very high CVD risk)',
    qrisk.warrants_statin_consideration AS warrants_statin_consideration WITH SYNONYMS = ('statin warranted') COMMENT = 'QRISK warrants statin consideration',
    
    -- Urine ACR Categories
    acr.acr_category AS acr_category COMMENT = 'ACR category (Normal/Moderate/Severe)',
    acr.is_acr_elevated AS is_acr_elevated COMMENT = 'ACR >=3 mg/mmol',
    acr.is_microalbuminuria AS is_microalbuminuria COMMENT = 'Microalbuminuria present',
    acr.is_macroalbuminuria AS is_macroalbuminuria COMMENT = 'Macroalbuminuria present'
)

METRICS(
    -- Population
    demographics.patient_count AS COUNT(DISTINCT demographics.person_id) COMMENT = 'Total patients',
    
    -- Blood Pressure
    bp.patients_with_bp AS COUNT(DISTINCT bp.person_id) COMMENT = 'Patients with BP measurement',
    bp_control.patients_with_bp_assessment AS COUNT(DISTINCT bp_control.person_id) COMMENT = 'Patients with BP control assessment',
    bp_control.bp_controlled_count AS COUNT(DISTINCT CASE WHEN bp_control.is_overall_bp_controlled THEN bp_control.person_id END) COMMENT = 'Patients with BP controlled',
    bp_control.bp_uncontrolled_count AS COUNT(DISTINCT CASE WHEN NOT bp_control.is_overall_bp_controlled THEN bp_control.person_id END) COMMENT = 'Patients with BP uncontrolled',
    bp_control.bp_stage_1_plus_count AS COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number >= 1 THEN bp_control.person_id END) COMMENT = 'Patients with Stage 1+ HTN',
    bp_control.bp_stage_2_plus_count AS COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number >= 2 THEN bp_control.person_id END) COMMENT = 'Patients with Stage 2+ HTN',
    bp_control.bp_stage_3_count AS COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number = 3 THEN bp_control.person_id END) COMMENT = 'Patients with Stage 3 (severe) HTN',
    bp_control.bp_case_finding_count AS COUNT(DISTINCT CASE WHEN bp_control.is_case_finding_candidate THEN bp_control.person_id END) COMMENT = 'BP case finding candidates',
    bp_control.bp_timely_count AS COUNT(DISTINCT CASE WHEN bp_control.is_latest_bp_within_recommended_interval THEN bp_control.person_id END) COMMENT = 'Patients with timely BP',
    
    -- HbA1c
    hba1c.patients_with_hba1c AS COUNT(DISTINCT hba1c.person_id) COMMENT = 'Patients with HbA1c',
    hba1c.hba1c_at_target_count AS COUNT(DISTINCT CASE WHEN hba1c.meets_qof_target THEN hba1c.person_id END) COMMENT = 'Patients with HbA1c at QOF target',
    hba1c.hba1c_above_target_count AS COUNT(DISTINCT CASE WHEN NOT hba1c.meets_qof_target AND hba1c.person_id IS NOT NULL THEN hba1c.person_id END) COMMENT = 'Patients with HbA1c above QOF target',
    hba1c.hba1c_high_risk_count AS COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Diabetes - High Risk' THEN hba1c.person_id END) COMMENT = 'Patients with HbA1c 75-85 (high risk)',
    hba1c.hba1c_very_high_risk_count AS COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Diabetes - Very High Risk' THEN hba1c.person_id END) COMMENT = 'Patients with HbA1c >=86 (very high risk)',
    hba1c.hba1c_poor_control_count AS COUNT(DISTINCT CASE WHEN hba1c.hba1c_category IN ('Diabetes - High Risk', 'Diabetes - Very High Risk') THEN hba1c.person_id END) COMMENT = 'Patients with HbA1c >=75 (poor control)',
    hba1c.hba1c_prediabetes_count AS COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Prediabetes' THEN hba1c.person_id END) COMMENT = 'Patients with prediabetes HbA1c',
    
    -- Cholesterol
    cholesterol.patients_with_cholesterol AS COUNT(DISTINCT cholesterol.person_id) COMMENT = 'Patients with cholesterol',
    cholesterol.cholesterol_desirable_count AS COUNT(DISTINCT CASE WHEN cholesterol.cholesterol_category = 'Desirable' THEN cholesterol.person_id END) COMMENT = 'Patients with desirable cholesterol',
    cholesterol.cholesterol_high_count AS COUNT(DISTINCT CASE WHEN cholesterol.cholesterol_category = 'High' THEN cholesterol.person_id END) COMMENT = 'Patients with high cholesterol',
    ldl.patients_with_ldl AS COUNT(DISTINCT ldl.person_id) COMMENT = 'Patients with LDL',
    ldl.ldl_at_target_count AS COUNT(DISTINCT CASE WHEN ldl.LDL_CVD_Target_Met = 'Met' THEN ldl.person_id END) COMMENT = 'Patients with LDL at target',
    
    -- BMI
    bmi.patients_with_bmi AS COUNT(DISTINCT bmi.person_id) COMMENT = 'Patients with BMI',
    bmi.bmi_normal_count AS COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Normal' THEN bmi.person_id END) COMMENT = 'Patients with normal BMI',
    bmi.bmi_overweight_count AS COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Overweight' THEN bmi.person_id END) COMMENT = 'Patients overweight',
    bmi.bmi_obese_count AS COUNT(DISTINCT CASE WHEN bmi.bmi_category IN ('Obese Class I', 'Obese Class II', 'Obese Class III') THEN bmi.person_id END) COMMENT = 'Patients with obesity',
    bmi.bmi_obese_class_3_count AS COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Obese Class III' THEN bmi.person_id END) COMMENT = 'Patients with severe obesity',
    bmi.bmi_underweight_count AS COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Underweight' THEN bmi.person_id END) COMMENT = 'Patients underweight',
    
    -- eGFR / CKD
    egfr.patients_with_egfr AS COUNT(DISTINCT egfr.person_id) COMMENT = 'Patients with eGFR',
    egfr.ckd_indicator_count AS COUNT(DISTINCT CASE WHEN egfr.is_ckd_indicator THEN egfr.person_id END) COMMENT = 'Patients with CKD indicator',
    egfr.ckd_stage_3_plus_count AS COUNT(DISTINCT CASE WHEN egfr.ckd_stage IN ('3a', '3b', '4', '5') THEN egfr.person_id END) COMMENT = 'Patients with CKD Stage 3+',
    egfr.ckd_stage_4_plus_count AS COUNT(DISTINCT CASE WHEN egfr.ckd_stage IN ('4', '5') THEN egfr.person_id END) COMMENT = 'Patients with CKD Stage 4-5',
    
    -- QRISK
    qrisk.patients_with_qrisk AS COUNT(DISTINCT qrisk.person_id) COMMENT = 'Patients with QRISK',
    qrisk.qrisk_high_risk_count AS COUNT(DISTINCT CASE WHEN qrisk.is_high_cvd_risk THEN qrisk.person_id END) COMMENT = 'Patients with QRISK >=10%',
    qrisk.qrisk_very_high_risk_count AS COUNT(DISTINCT CASE WHEN qrisk.is_very_high_cvd_risk THEN qrisk.person_id END) COMMENT = 'Patients with QRISK >=20%',
    qrisk.qrisk_statin_count AS COUNT(DISTINCT CASE WHEN qrisk.warrants_statin_consideration THEN qrisk.person_id END) COMMENT = 'Patients where statin warranted',
    
    -- Urine ACR
    acr.patients_with_acr AS COUNT(DISTINCT acr.person_id) COMMENT = 'Patients with ACR',
    acr.acr_elevated_count AS COUNT(DISTINCT CASE WHEN acr.is_acr_elevated THEN acr.person_id END) COMMENT = 'Patients with elevated ACR',
    acr.microalbuminuria_count AS COUNT(DISTINCT CASE WHEN acr.is_microalbuminuria THEN acr.person_id END) COMMENT = 'Patients with microalbuminuria',
    acr.macroalbuminuria_count AS COUNT(DISTINCT CASE WHEN acr.is_macroalbuminuria THEN acr.person_id END) COMMENT = 'Patients with macroalbuminuria',
    
    -- Averages
    bp.avg_systolic_bp AS AVG(bp.systolic_value) COMMENT = 'Average systolic BP',
    bp.avg_diastolic_bp AS AVG(bp.diastolic_value) COMMENT = 'Average diastolic BP',
    hba1c.avg_hba1c AS AVG(hba1c.hba1c_ifcc) COMMENT = 'Average HbA1c',
    cholesterol.avg_cholesterol AS AVG(cholesterol.cholesterol_value) COMMENT = 'Average cholesterol',
    ldl.avg_ldl AS AVG(ldl.cholesterol_value) COMMENT = 'Average LDL',
    bmi.avg_bmi AS AVG(bmi.bmi_value) COMMENT = 'Average BMI',
    egfr.avg_egfr AS AVG(egfr.egfr_value) COMMENT = 'Average eGFR',
    qrisk.avg_qrisk AS AVG(qrisk.qrisk_score) COMMENT = 'Average QRISK'
)

COMMENT = 'OLIDS Clinical Observations Semantic View - Clinical biomarkers with category-based metrics for population health. Includes patient-specific BP thresholds. Grain: one row per person (latest values).'
AI_SQL_GENERATION 'Always filter to is_active = TRUE unless asked otherwise. For BP control queries, use bp_controlled_count and patients_with_bp_assessment to calculate control rate. Prefer category-based counts over averages for population health questions. BP control uses patient-specific thresholds based on T2DM, CKD, and age.'
AI_QUESTION_CATEGORIZATION 'Use this view for questions about: BP control, HbA1c control, cholesterol, BMI, eGFR, CKD staging, QRISK, ACR, and clinical biomarkers. For condition prevalence and demographics use sem_olids_population. For trends over time use sem_olids_trends.'
