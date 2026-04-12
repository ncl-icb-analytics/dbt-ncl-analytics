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
    clinically meaningful categories and control status. OLIDS is the
    One London Integrated Data Set — primary care data from system
    suppliers (currently EMIS Web, with TPP to follow), unified by the
    One London team.

    Grain: One row per person (latest observation values)

    Design Principles:
    - Categories over averages (% at target vs mean value)
    - Patient-specific thresholds where applicable (BP control)
    - Pre-computed clinical classifications from int/fct models

    Observation Groups:
    - Cardiovascular: BP, BP control, cholesterol, LDL, QRISK
    - Metabolic: HbA1c, BMI, waist circumference
    - Renal: eGFR (CKD staging), creatinine, urine ACR
    - Frailty: Electronic Frailty Index (eFI/eFI2), Rockwood Clinical Frailty Scale
    - Diabetes care: Foot examination, retinal screening, DM 8 care processes
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

    waist AS {{ ref('int_waist_circumference_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest waist circumference with risk categories',

    egfr AS {{ ref('int_egfr_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest eGFR with CKD staging',

    creatinine AS {{ ref('int_creatinine_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest serum creatinine measurement',

    qrisk AS {{ ref('int_qrisk_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest QRISK cardiovascular risk score',

    acr AS {{ ref('int_urine_acr_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest urine albumin:creatinine ratio',

    efi AS {{ ref('int_efi_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest Electronic Frailty Index (eFI or eFI2). Only includes scores explicitly coded by the GP — not dynamically calculated. May be out of date or absent for many patients. For frailty prevalence, prefer has_frailty from sem_olids_population (clinical diagnosis register).',

    rockwood AS {{ ref('int_rockwood_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest Rockwood Clinical Frailty Scale score (1-9)',

    foot_exam AS {{ ref('int_foot_examination_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest diabetic foot examination with risk levels per foot',

    retinal AS {{ ref('int_retinal_screening_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest completed diabetic retinal screening',

    dm8cp AS {{ ref('fct_person_diabetes_8_care_processes') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Diabetes 8 care processes completion status (12-month lookback). Only populated for persons on the diabetes register.',

    esp AS {{ ref('esp_2013') }}
        PRIMARY KEY (age_band_esp)
        COMMENT = 'European Standard Population 2013 weights for age-standardised rate calculation (Eurostat revision, used by ONS/OHID)'
)

RELATIONSHIPS(
    bp (person_id) REFERENCES demographics,
    bp_control (person_id) REFERENCES demographics,
    hba1c (person_id) REFERENCES demographics,
    cholesterol (person_id) REFERENCES demographics,
    ldl (person_id) REFERENCES demographics,
    bmi (person_id) REFERENCES demographics,
    waist (person_id) REFERENCES demographics,
    egfr (person_id) REFERENCES demographics,
    creatinine (person_id) REFERENCES demographics,
    qrisk (person_id) REFERENCES demographics,
    acr (person_id) REFERENCES demographics,
    efi (person_id) REFERENCES demographics,
    rockwood (person_id) REFERENCES demographics,
    foot_exam (person_id) REFERENCES demographics,
    retinal (person_id) REFERENCES demographics,
    dm8cp (person_id) REFERENCES demographics,
    demographics (age_band_esp) REFERENCES esp
)

FACTS(
    -- Cardiovascular
    bp.systolic_value AS systolic_value COMMENT = 'Systolic BP (mmHg)',
    bp.diastolic_value AS diastolic_value COMMENT = 'Diastolic BP (mmHg)',
    cholesterol.cholesterol_value AS cholesterol_value COMMENT = 'Total cholesterol (mmol/L)',
    ldl.cholesterol_value AS cholesterol_value COMMENT = 'LDL cholesterol (mmol/L)',
    qrisk.qrisk_score AS qrisk_score WITH SYNONYMS = ('CVD risk', 'cardiovascular risk') COMMENT = 'QRISK score (%)',
    bp_control.latest_bp_reading_age_months AS latest_bp_reading_age_months COMMENT = 'Months since last BP reading',

    -- Metabolic
    hba1c.hba1c_ifcc AS hba1c_ifcc COMMENT = 'HbA1c value (mmol/mol IFCC)',
    bmi.bmi_value AS bmi_value COMMENT = 'BMI value (kg/m2)',
    waist.waist_circumference_value AS waist_circumference_value COMMENT = 'Waist circumference (cm)',

    -- Renal
    egfr.egfr_value AS egfr_value COMMENT = 'eGFR value (mL/min/1.73m2)',
    creatinine.creatinine_value AS creatinine_value COMMENT = 'Serum creatinine (umol/L)',
    acr.acr_value AS acr_value COMMENT = 'Urine ACR (mg/mmol)',

    -- Frailty
    efi.latest_efi_score_preferred AS latest_efi_score_preferred COMMENT = 'Electronic Frailty Index score (0-1). Uses most recent of eFI or eFI2.',
    rockwood.rockwood_score AS rockwood_score COMMENT = 'Rockwood Clinical Frailty Scale score (1-9)',

    -- Diabetes 8 Care Processes
    dm8cp.care_processes_completed AS care_processes_completed COMMENT = 'Count of diabetes 8 care processes completed in last 12 months (0-8). Only for persons on diabetes register.',

    -- ESP
    esp.esp_weight AS esp_weight COMMENT = 'ESP 2013 population weight for this age band (out of 100,000 total). Use with age_band_esp for age-standardised rate calculation.',
    esp.esp_proportion AS esp_proportion COMMENT = 'ESP 2013 weight as proportion (esp_weight / 100,000). Multiply stratum-specific rate by this and SUM across bands to get the ASR.'
)

DIMENSIONS(
    -- Observation Dates
    bp.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest BP reading',
    hba1c.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest HbA1c',
    cholesterol.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest cholesterol',
    ldl.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest LDL',
    bmi.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest BMI',
    waist.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest waist circumference',
    egfr.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest eGFR',
    creatinine.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest creatinine',
    qrisk.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest QRISK',
    acr.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest ACR',
    efi.clinical_effective_date AS latest_efi_date COMMENT = 'Date of latest eFI assessment',
    rockwood.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest Rockwood assessment',
    foot_exam.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest foot examination',
    retinal.clinical_effective_date AS clinical_effective_date COMMENT = 'Date of latest retinal screening',

    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age AS age COMMENT = 'Current age in years',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands (0-4, 5-9, ..., 80-84, 85+, Unknown)',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands (0-9, 10-19, ..., 70-79, 80+, Unknown)',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS Digital standard age bands (0-4, 5-14, 15-24, ..., 75-84, 85+)',
    demographics.age_band_esp AS age_band_esp COMMENT = 'ESP 2013 age bands (<1, 1-4, 5-9, ..., 80-84, 85-89, 90-94, 95+). Join to esp_weight for standardised rates.',
    demographics.age_life_stage AS age_life_stage COMMENT = 'Life stage (Infant, Toddler, Child, Adolescent, Young Adult, Adult, Older Adult, Elderly, Very Elderly, Unknown)',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (White: British, White: Irish, White: Roma, White: Traveller, White: Other White, Mixed: White and Black Caribbean, Mixed: White and Black African, Mixed: White and Asian, Mixed: Other Mixed, Asian: Indian, Asian: Pakistani, Asian: Bangladeshi, Asian: Chinese, Asian: Other Asian, Black: African, Black: Caribbean, Black: Other Black, Other: Arab, Other: Other, Unknown, Not Stated, Not Recorded, Recorded Not Known, Refused)',
    demographics.is_active AS is_active COMMENT = 'Currently registered',

    -- Organisation
    demographics.practice_code AS practice_code COMMENT = 'GP practice ODS code',
    demographics.practice_name AS practice_name COMMENT = 'GP practice name',
    demographics.pcn_code AS pcn_code COMMENT = 'Primary Care Network code',
    demographics.pcn_name AS pcn_name COMMENT = 'Primary Care Network name',
    demographics.borough_registered AS borough_registered COMMENT = 'Registration borough',
    demographics.neighbourhood_registered AS neighbourhood_registered COMMENT = 'Registration neighbourhood',

    -- Geography (residence)
    demographics.lsoa_code_21 AS lsoa_code_21 COMMENT = 'Lower Super Output Area 2021 code',
    demographics.ward_code AS ward_code COMMENT = 'Electoral ward 2025 code',
    demographics.ward_name AS ward_name COMMENT = 'Electoral ward 2025 name',
    demographics.borough_resident AS borough_resident COMMENT = 'Residence borough',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Residence neighbourhood',

    -- Deprivation
    demographics.imd_decile_19 AS imd_decile_19 COMMENT = 'IMD 2019 decile (1=most deprived, 10=least). NULL if LSOA not mapped.',
    demographics.imd_quintile_19 AS imd_quintile_19 COMMENT = 'IMD 2019 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least). Preferred over 2019.',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',

    -- Blood Pressure (raw)
    bp.is_home_bp_event AS is_home_bp_event WITH SYNONYMS = ('HBPM', 'home monitoring', 'home BP') COMMENT = 'Home BP measurement',
    bp.is_abpm_bp_event AS is_abpm_bp_event WITH SYNONYMS = ('ABPM', 'ambulatory', '24-hour') COMMENT = 'Ambulatory BP measurement',
    bp.is_hypertensive_range AS is_hypertensive_range COMMENT = 'BP in hypertensive range',

    -- Blood Pressure Control
    bp_control.is_overall_bp_controlled AS is_overall_bp_controlled WITH SYNONYMS = ('BP at target', 'BP controlled', 'controlled') COMMENT = 'BP controlled (patient-specific threshold)',
    bp_control.is_systolic_controlled AS is_systolic_controlled COMMENT = 'Systolic BP at target',
    bp_control.is_diastolic_controlled AS is_diastolic_controlled COMMENT = 'Diastolic BP at target',
    bp_control.hypertension_stage AS hypertension_stage COMMENT = 'Hypertension stage (Normal, Stage 1, Stage 2, Stage 3 Severe)',
    bp_control.hypertension_stage_number AS hypertension_stage_number COMMENT = 'Hypertension stage number (0-3)',
    bp_control.applied_patient_group AS applied_patient_group WITH SYNONYMS = ('BP threshold group') COMMENT = 'Which threshold applied (T2DM, CKD, AGE_GE_80, STANDARD)',
    bp_control.is_case_finding_candidate AS is_case_finding_candidate WITH SYNONYMS = ('BP case finding') COMMENT = 'Elevated BP but not on HTN register',
    bp_control.is_latest_bp_within_recommended_interval AS is_latest_bp_within_recommended_interval WITH SYNONYMS = ('BP timely', 'timely BP') COMMENT = 'BP within recommended interval',
    bp_control.has_t2dm AS has_t2dm COMMENT = 'Has Type 2 diabetes (affects BP threshold)',
    bp_control.has_ckd AS has_ckd COMMENT = 'Has CKD (affects BP threshold)',
    bp_control.is_diagnosed_htn AS is_diagnosed_htn WITH SYNONYMS = ('on HTN register', 'diagnosed hypertension') COMMENT = 'On hypertension register',

    -- HbA1c Categories
    hba1c.hba1c_category AS hba1c_category COMMENT = 'HbA1c category (Normal, Prediabetes, Diabetes - At NICE Target, Diabetes - Acceptable, Diabetes - Above Target, Diabetes - High Risk, Diabetes - Very High Risk)',
    hba1c.meets_qof_target AS meets_qof_target WITH SYNONYMS = ('HbA1c controlled', 'at target') COMMENT = 'HbA1c <=58 mmol/mol (QOF target)',
    hba1c.indicates_diabetes AS indicates_diabetes COMMENT = 'HbA1c >=48 mmol/mol (diabetes diagnostic)',

    -- Cholesterol Categories
    cholesterol.cholesterol_category AS cholesterol_category COMMENT = 'Cholesterol category (Desirable, Borderline, High)',
    ldl.LDL_CVD_Target_Met AS LDL_CVD_Target_Met COMMENT = 'LDL at CVD target (Met, Not Met)',

    -- BMI Categories
    bmi.bmi_category AS bmi_category COMMENT = 'BMI category (Underweight, Normal, Overweight, Obese Class I, Obese Class II, Obese Class III). Uses ethnicity-adjusted thresholds per NICE NG246.',
    bmi.requires_lower_bmi_thresholds AS requires_lower_bmi_thresholds COMMENT = 'Uses lower BMI thresholds for ethnicity',
    bmi.is_valid_bmi AS is_valid_bmi COMMENT = 'BMI in valid range',

    -- Waist Circumference Categories
    waist.waist_risk_category AS waist_risk_category COMMENT = 'Waist circumference risk (Low Risk, Moderate Risk Female, Moderate Risk, High Risk, Very High Risk)',
    waist.is_high_waist_risk AS is_high_waist_risk COMMENT = 'Waist >=88cm',
    waist.is_very_high_waist_risk AS is_very_high_waist_risk COMMENT = 'Waist >=102cm',

    -- eGFR / CKD Staging
    egfr.ckd_stage AS ckd_stage COMMENT = 'CKD stage (1, 2, 3a, 3b, 4, 5)',
    egfr.is_ckd_indicator AS is_ckd_indicator COMMENT = 'eGFR indicates CKD',

    -- Creatinine Categories
    creatinine.creatinine_category AS creatinine_category COMMENT = 'Creatinine category (Normal, Mildly Elevated, Moderately Elevated, Severely Elevated)',
    creatinine.is_elevated_creatinine AS is_elevated_creatinine COMMENT = 'Creatinine >120 umol/L',

    -- QRISK Categories
    qrisk.qrisk_type AS qrisk_type COMMENT = 'QRISK version (QRISK2, QRISK3)',
    qrisk.cvd_risk_category AS cvd_risk_category COMMENT = 'CVD risk category (Low, Moderate, High, Very High)',
    qrisk.is_high_cvd_risk AS is_high_cvd_risk WITH SYNONYMS = ('high risk', 'QRISK >= 10') COMMENT = 'QRISK >=10% (high CVD risk)',
    qrisk.is_very_high_cvd_risk AS is_very_high_cvd_risk COMMENT = 'QRISK >=20% (very high CVD risk)',
    qrisk.warrants_statin_consideration AS warrants_statin_consideration WITH SYNONYMS = ('statin warranted') COMMENT = 'QRISK warrants statin consideration',

    -- Urine ACR Categories
    acr.acr_category AS acr_category COMMENT = 'ACR category (Normal, Moderate, Severe)',
    acr.is_acr_elevated AS is_acr_elevated COMMENT = 'ACR >=3 mg/mmol',
    acr.is_microalbuminuria AS is_microalbuminuria COMMENT = 'Microalbuminuria present',
    acr.is_macroalbuminuria AS is_macroalbuminuria COMMENT = 'Macroalbuminuria present',

    -- Electronic Frailty Index
    efi.latest_efi_type_preferred AS latest_efi_type_preferred COMMENT = 'eFI algorithm type (EFI, EFI2). Uses most recent available. Only where explicitly GP-coded.',
    efi.latest_efi_category_preferred AS latest_efi_category_preferred WITH SYNONYMS = ('frailty category', 'eFI category') COMMENT = 'eFI frailty category (Fit, Mildly Frail, Moderately Frail, Severely Frail). Only where explicitly GP-coded — not dynamically estimated. Coverage is incomplete. For population frailty prevalence, use has_frailty from sem_olids_population instead.',

    -- Rockwood Clinical Frailty Scale
    rockwood.frailty_category AS frailty_category WITH SYNONYMS = ('Rockwood category', 'CFS category') COMMENT = 'Rockwood frailty category (Fit, Vulnerable, Mild Frailty, Moderate Frailty, Severe Frailty)',
    rockwood.is_frail AS is_frail COMMENT = 'Rockwood score >=5 (frail)',
    rockwood.is_severely_frail AS is_severely_frail COMMENT = 'Rockwood score >=7 (severely frail)',

    -- Diabetic Foot Examination
    foot_exam.both_feet_checked AS both_feet_checked COMMENT = 'Both feet examined',
    foot_exam.left_foot_risk_level AS left_foot_risk_level COMMENT = 'Left foot risk (Low, Moderate, High, Ulcerated)',
    foot_exam.right_foot_risk_level AS right_foot_risk_level COMMENT = 'Right foot risk (Low, Moderate, High, Ulcerated)',
    foot_exam.is_unsuitable AS is_unsuitable COMMENT = 'Patient unsuitable for foot exam',
    foot_exam.is_declined AS is_declined COMMENT = 'Patient declined foot exam',

    -- Diabetic Retinal Screening
    retinal.screening_current_12m AS screening_current_12m COMMENT = 'Retinal screening completed in last 12 months',
    retinal.screening_current_24m AS screening_current_24m COMMENT = 'Retinal screening completed in last 24 months',

    -- Diabetes 8 Care Processes (only populated for persons on diabetes register)
    dm8cp.hba1c_completed_in_last_12m AS hba1c_completed_in_last_12m WITH SYNONYMS = ('DM HbA1c check') COMMENT = 'DM care process: HbA1c in last 12m',
    dm8cp.bp_completed_in_last_12m AS bp_completed_in_last_12m WITH SYNONYMS = ('DM BP check') COMMENT = 'DM care process: BP in last 12m',
    dm8cp.cholesterol_completed_in_last_12m AS cholesterol_completed_in_last_12m WITH SYNONYMS = ('DM cholesterol check') COMMENT = 'DM care process: cholesterol in last 12m',
    dm8cp.creatinine_completed_in_last_12m AS creatinine_completed_in_last_12m WITH SYNONYMS = ('DM creatinine check') COMMENT = 'DM care process: serum creatinine in last 12m',
    dm8cp.acr_completed_in_last_12m AS acr_completed_in_last_12m WITH SYNONYMS = ('DM ACR check') COMMENT = 'DM care process: urine ACR in last 12m',
    dm8cp.foot_check_completed_in_last_12m AS foot_check_completed_in_last_12m WITH SYNONYMS = ('DM foot check') COMMENT = 'DM care process: foot exam in last 12m',
    dm8cp.bmi_completed_in_last_12m AS bmi_completed_in_last_12m WITH SYNONYMS = ('DM BMI check') COMMENT = 'DM care process: BMI in last 12m',
    dm8cp.smoking_completed_in_last_12m AS smoking_completed_in_last_12m WITH SYNONYMS = ('DM smoking check') COMMENT = 'DM care process: smoking status in last 12m',
    dm8cp.all_processes_completed AS all_processes_completed WITH SYNONYMS = ('all 8 care processes', 'DM 8CP') COMMENT = 'All 8 diabetes care processes completed in last 12m'
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

    -- Waist Circumference
    waist.patients_with_waist AS COUNT(DISTINCT waist.person_id) COMMENT = 'Patients with waist measurement',
    waist.waist_high_risk_count AS COUNT(DISTINCT CASE WHEN waist.is_high_waist_risk THEN waist.person_id END) COMMENT = 'Patients with high waist risk (>=88cm)',
    waist.waist_very_high_risk_count AS COUNT(DISTINCT CASE WHEN waist.is_very_high_waist_risk THEN waist.person_id END) COMMENT = 'Patients with very high waist risk (>=102cm)',

    -- eGFR / CKD
    egfr.patients_with_egfr AS COUNT(DISTINCT egfr.person_id) COMMENT = 'Patients with eGFR',
    egfr.ckd_indicator_count AS COUNT(DISTINCT CASE WHEN egfr.is_ckd_indicator THEN egfr.person_id END) COMMENT = 'Patients with CKD indicator',
    egfr.ckd_stage_3_plus_count AS COUNT(DISTINCT CASE WHEN egfr.ckd_stage IN ('3a', '3b', '4', '5') THEN egfr.person_id END) COMMENT = 'Patients with CKD Stage 3+',
    egfr.ckd_stage_4_plus_count AS COUNT(DISTINCT CASE WHEN egfr.ckd_stage IN ('4', '5') THEN egfr.person_id END) COMMENT = 'Patients with CKD Stage 4-5',

    -- Creatinine
    creatinine.patients_with_creatinine AS COUNT(DISTINCT creatinine.person_id) COMMENT = 'Patients with creatinine',
    creatinine.creatinine_elevated_count AS COUNT(DISTINCT CASE WHEN creatinine.is_elevated_creatinine THEN creatinine.person_id END) COMMENT = 'Patients with elevated creatinine',

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

    -- Frailty (eFI)
    efi.patients_with_efi AS COUNT(DISTINCT efi.person_id) COMMENT = 'Patients with eFI assessment',
    efi.efi_mildly_frail_count AS COUNT(DISTINCT CASE WHEN efi.latest_efi_category_preferred = 'Mildly Frail' THEN efi.person_id END) COMMENT = 'Patients mildly frail (eFI)',
    efi.efi_moderately_frail_count AS COUNT(DISTINCT CASE WHEN efi.latest_efi_category_preferred = 'Moderately Frail' THEN efi.person_id END) COMMENT = 'Patients moderately frail (eFI)',
    efi.efi_severely_frail_count AS COUNT(DISTINCT CASE WHEN efi.latest_efi_category_preferred = 'Severely Frail' THEN efi.person_id END) COMMENT = 'Patients severely frail (eFI)',

    -- Frailty (Rockwood)
    rockwood.patients_with_rockwood AS COUNT(DISTINCT rockwood.person_id) COMMENT = 'Patients with Rockwood assessment',
    rockwood.rockwood_frail_count AS COUNT(DISTINCT CASE WHEN rockwood.is_frail THEN rockwood.person_id END) COMMENT = 'Patients frail (Rockwood >=5)',
    rockwood.rockwood_severely_frail_count AS COUNT(DISTINCT CASE WHEN rockwood.is_severely_frail THEN rockwood.person_id END) COMMENT = 'Patients severely frail (Rockwood >=7)',

    -- Diabetic Foot Examination
    foot_exam.patients_with_foot_exam AS COUNT(DISTINCT foot_exam.person_id) COMMENT = 'Patients with foot examination',

    -- Diabetic Retinal Screening
    retinal.patients_with_retinal AS COUNT(DISTINCT retinal.person_id) COMMENT = 'Patients with retinal screening',
    retinal.retinal_current_12m_count AS COUNT(DISTINCT CASE WHEN retinal.screening_current_12m THEN retinal.person_id END) COMMENT = 'Patients with retinal screening in last 12m',

    -- Diabetes 8 Care Processes
    dm8cp.dm_patients_count AS COUNT(DISTINCT dm8cp.person_id) COMMENT = 'Patients on diabetes register (in DM 8CP model)',
    dm8cp.dm_all_8cp_count AS COUNT(DISTINCT CASE WHEN dm8cp.all_processes_completed THEN dm8cp.person_id END) COMMENT = 'DM patients with all 8 care processes completed',

    -- Averages
    bp.avg_systolic_bp AS AVG(bp.systolic_value) COMMENT = 'Average systolic BP',
    bp.avg_diastolic_bp AS AVG(bp.diastolic_value) COMMENT = 'Average diastolic BP',
    hba1c.avg_hba1c AS AVG(hba1c.hba1c_ifcc) COMMENT = 'Average HbA1c',
    cholesterol.avg_cholesterol AS AVG(cholesterol.cholesterol_value) COMMENT = 'Average cholesterol',
    ldl.avg_ldl AS AVG(ldl.cholesterol_value) COMMENT = 'Average LDL',
    bmi.avg_bmi AS AVG(bmi.bmi_value) COMMENT = 'Average BMI',
    egfr.avg_egfr AS AVG(egfr.egfr_value) COMMENT = 'Average eGFR',
    qrisk.avg_qrisk AS AVG(qrisk.qrisk_score) COMMENT = 'Average QRISK',
    acr.avg_acr AS AVG(acr.acr_value) COMMENT = 'Average ACR',
    efi.avg_efi_score AS AVG(efi.latest_efi_score_preferred) COMMENT = 'Average eFI score',
    rockwood.avg_rockwood AS AVG(rockwood.rockwood_score) COMMENT = 'Average Rockwood score',
    dm8cp.avg_dm_care_processes AS AVG(dm8cp.care_processes_completed) COMMENT = 'Average DM care processes completed (of 8)'
)

COMMENT = 'OLIDS Clinical Observations Semantic View - Biomarkers, frailty scores, diabetes care processes, and screening with category-based metrics. Includes patient-specific BP thresholds, eFI/Rockwood frailty, DM 8 care processes, foot exam and retinal screening. Grain: one row per person (latest values). ESP 2013 weights available via age_band_esp.'
AI_SQL_GENERATION 'Always filter to is_active = TRUE unless asked otherwise. For BP control queries, use bp_controlled_count and patients_with_bp_assessment to calculate control rate. Prefer category-based counts over averages for population health questions. BP control uses patient-specific thresholds based on T2DM, CKD, and age. DM 8 care processes (dm8cp table) are only populated for persons on the diabetes register — filter to dm8cp metrics when analysing diabetes care. The 8 processes are: HbA1c, BP, cholesterol, creatinine, urine ACR, foot exam, BMI, and smoking status — each checked within last 12 months. AGE-STANDARDISED RATES: To calculate an age-standardised rate (ASR) using ESP 2013 (the standard used by ONS/OHID/Fingertips), use this pattern: WITH strata AS (SELECT <area_column>, age_band_esp, COUNT(DISTINCT CASE WHEN <condition_or_category> THEN person_id END) AS cases, COUNT(DISTINCT person_id) AS pop, ANY_VALUE(esp_proportion) AS esp_prop FROM <this_view> WHERE is_active = TRUE GROUP BY <area_column>, age_band_esp) SELECT <area_column>, SUM(cases) AS crude_cases, SUM(pop) AS crude_pop, ROUND(SUM((cases / NULLIF(pop, 0)) * esp_prop) * 100000, 1) AS asr_per_100k FROM strata GROUP BY <area_column>. For internal NCL comparison instead of ESP, replace esp_prop with (pop / SUM(pop) OVER ()) to use the NCL population structure as the standard.'
AI_QUESTION_CATEGORIZATION 'Use this view for questions about: BP control, HbA1c control, cholesterol, BMI, waist circumference, eGFR, CKD staging, creatinine, QRISK, ACR, frailty (eFI, Rockwood), diabetic foot examination, retinal screening, diabetes 8 care processes, and all clinical biomarkers. For condition prevalence and demographics use sem_olids_population. For trends over time use sem_olids_trends.'
