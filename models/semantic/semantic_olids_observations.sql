{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Clinical Observations Semantic View
    ==========================================
    
    Semantic model for clinical observations and biomarkers including
    blood pressure, HbA1c, cholesterol, BMI, eGFR, and cardiovascular risk.
    
    Grain: One row per person (latest observation values)
    
    Note: Observation tables only contain rows for people WITH that measurement.
    This semantic view uses dim_person_demographics as the base table with
    LEFT JOINs to observation tables, so some observation dimensions/facts
    may be NULL for patients without that specific measurement.
    
    Tables:
    - dim_person_demographics: Base population (all persons)
    - int_blood_pressure_latest: Latest BP (systolic/diastolic)
    - int_hba1c_latest: Latest HbA1c (glycaemic control)
    - int_cholesterol_latest: Latest total cholesterol
    - int_cholesterol_ldl_latest: Latest LDL cholesterol
    - int_bmi_latest: Latest BMI with ethnicity-adjusted categories
    - int_egfr_latest: Latest eGFR with CKD staging
    - int_qrisk_latest: Latest QRISK score (CVD risk)
    - int_urine_acr_latest: Latest urine ACR (kidney function)
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Base population - all persons with registration history',
    
    bp AS {{ ref('int_blood_pressure_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest blood pressure measurement (systolic/diastolic)',
    
    hba1c AS {{ ref('int_hba1c_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest HbA1c measurement for glycaemic control',
    
    cholesterol AS {{ ref('int_cholesterol_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest total cholesterol measurement',
    
    ldl AS {{ ref('int_cholesterol_ldl_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest LDL cholesterol measurement',
    
    bmi AS {{ ref('int_bmi_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest BMI with ethnicity-adjusted categorisation',
    
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
    hba1c (person_id) REFERENCES demographics,
    cholesterol (person_id) REFERENCES demographics,
    ldl (person_id) REFERENCES demographics,
    bmi (person_id) REFERENCES demographics,
    egfr (person_id) REFERENCES demographics,
    qrisk (person_id) REFERENCES demographics,
    acr (person_id) REFERENCES demographics
)

FACTS(
    -- Blood Pressure Values
    bp.systolic_value COMMENT = 'Systolic BP (mmHg)',
    bp.diastolic_value COMMENT = 'Diastolic BP (mmHg)',
    
    -- HbA1c Values
    hba1c.hba1c_ifcc COMMENT = 'HbA1c value (mmol/mol IFCC)',
    
    -- Cholesterol Values
    cholesterol.cholesterol_value COMMENT = 'Total cholesterol (mmol/L)',
    ldl.cholesterol_value COMMENT = 'LDL cholesterol (mmol/L)',
    
    -- BMI Values
    bmi.bmi_value COMMENT = 'BMI value (kg/m²)',
    
    -- eGFR Values
    egfr.egfr_value COMMENT = 'eGFR value (mL/min/1.73m²)',
    
    -- QRISK Values
    qrisk.qrisk_score COMMENT = 'QRISK score (%)',
    
    -- Urine ACR Values
    acr.acr_value COMMENT = 'Urine ACR (mg/mmol)'
)

DIMENSIONS(
    -- Core Demographics (for segmentation - age in FACTS for aggregation)
    demographics.gender COMMENT = 'Patient gender',
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
    
    -- Blood Pressure
    bp.is_home_bp_event COMMENT = 'Home BP measurement',
    bp.is_abpm_bp_event COMMENT = 'Ambulatory BP measurement',
    bp.is_hypertensive_range COMMENT = 'BP in hypertensive range (≥140/90 or ≥135/85 ABPM)',
    
    -- HbA1c
    hba1c.hba1c_category COMMENT = 'HbA1c control category',
    hba1c.meets_qof_target COMMENT = 'HbA1c ≤58 mmol/mol (QOF target)',
    hba1c.indicates_diabetes COMMENT = 'HbA1c ≥48 mmol/mol (diabetes diagnostic)',
    
    -- Cholesterol
    cholesterol.cholesterol_category COMMENT = 'Cholesterol category (Desirable/Borderline/High)',
    
    -- LDL Cholesterol
    ldl.LDL_CVD_Target_Met COMMENT = 'LDL cholesterol at CVD target',
    
    -- BMI
    bmi.bmi_category COMMENT = 'BMI category (ethnicity-adjusted per NICE NG246)',
    bmi.requires_lower_bmi_thresholds COMMENT = 'Uses lower BMI thresholds for cardiometabolic risk',
    bmi.is_valid_bmi COMMENT = 'BMI in valid range',
    
    -- eGFR / CKD
    egfr.ckd_stage COMMENT = 'CKD stage (1-5)',
    egfr.is_ckd_indicator COMMENT = 'eGFR indicates CKD',
    
    -- QRISK
    qrisk.qrisk_type COMMENT = 'QRISK version (QRISK2/QRISK3)',
    qrisk.cvd_risk_category COMMENT = 'CVD risk category',
    qrisk.is_high_cvd_risk COMMENT = 'QRISK ≥10% (high CVD risk)',
    qrisk.is_very_high_cvd_risk COMMENT = 'QRISK ≥20% (very high CVD risk)',
    qrisk.warrants_statin_consideration COMMENT = 'QRISK warrants statin consideration',
    
    -- Urine ACR
    acr.acr_category COMMENT = 'ACR category (Normal/Moderate/Severe)',
    acr.is_acr_elevated COMMENT = 'ACR ≥3 mg/mmol',
    acr.is_microalbuminuria COMMENT = 'Microalbuminuria present',
    acr.is_macroalbuminuria COMMENT = 'Macroalbuminuria present'
)

METRICS(
    -- Population Counts
    COUNT(DISTINCT demographics.person_id) AS patient_count
        COMMENT = 'Total patients',
    
    -- Blood Pressure Metrics
    COUNT(DISTINCT bp.person_id) AS patients_with_bp
        COMMENT = 'Patients with BP measurement',
    
    AVG(bp.systolic_value) AS avg_systolic_bp
        COMMENT = 'Average systolic BP',
    
    AVG(bp.diastolic_value) AS avg_diastolic_bp
        COMMENT = 'Average diastolic BP',
    
    COUNT(DISTINCT CASE WHEN bp.is_hypertensive_range THEN demographics.person_id END) AS hypertensive_range_count
        COMMENT = 'Patients with BP in hypertensive range',
    
    -- HbA1c Metrics
    COUNT(DISTINCT hba1c.person_id) AS patients_with_hba1c
        COMMENT = 'Patients with HbA1c measurement',
    
    AVG(hba1c.hba1c_ifcc) AS avg_hba1c
        COMMENT = 'Average HbA1c (mmol/mol)',
    
    COUNT(DISTINCT CASE WHEN hba1c.meets_qof_target THEN demographics.person_id END) AS hba1c_controlled_count
        COMMENT = 'Patients with HbA1c ≤58 (QOF controlled)',
    
    COUNT(DISTINCT CASE WHEN hba1c.indicates_diabetes THEN demographics.person_id END) AS hba1c_diabetes_range_count
        COMMENT = 'Patients with HbA1c ≥48 (diabetes range)',
    
    -- Cholesterol Metrics
    COUNT(DISTINCT cholesterol.person_id) AS patients_with_cholesterol
        COMMENT = 'Patients with cholesterol measurement',
    
    AVG(cholesterol.cholesterol_value) AS avg_cholesterol
        COMMENT = 'Average total cholesterol (mmol/L)',
    
    COUNT(DISTINCT CASE WHEN cholesterol.cholesterol_category = 'Desirable' THEN demographics.person_id END) AS cholesterol_desirable_count
        COMMENT = 'Patients with desirable cholesterol (<5 mmol/L)',
    
    -- LDL Metrics
    COUNT(DISTINCT ldl.person_id) AS patients_with_ldl
        COMMENT = 'Patients with LDL measurement',
    
    AVG(ldl.cholesterol_value) AS avg_ldl
        COMMENT = 'Average LDL cholesterol (mmol/L)',
    
    COUNT(DISTINCT CASE WHEN ldl.LDL_CVD_Target_Met THEN demographics.person_id END) AS ldl_at_target_count
        COMMENT = 'Patients with LDL at CVD target',
    
    -- BMI Metrics
    COUNT(DISTINCT bmi.person_id) AS patients_with_bmi
        COMMENT = 'Patients with BMI measurement',
    
    AVG(bmi.bmi_value) AS avg_bmi
        COMMENT = 'Average BMI (kg/m²)',
    
    COUNT(DISTINCT CASE WHEN bmi.bmi_category IN ('Obese Class I', 'Obese Class II', 'Obese Class III') THEN demographics.person_id END) AS obese_count
        COMMENT = 'Patients with obesity (any class)',
    
    COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Overweight' THEN demographics.person_id END) AS overweight_count
        COMMENT = 'Patients who are overweight',
    
    -- eGFR / CKD Metrics
    COUNT(DISTINCT egfr.person_id) AS patients_with_egfr
        COMMENT = 'Patients with eGFR measurement',
    
    AVG(egfr.egfr_value) AS avg_egfr
        COMMENT = 'Average eGFR (mL/min/1.73m²)',
    
    COUNT(DISTINCT CASE WHEN egfr.is_ckd_indicator THEN demographics.person_id END) AS ckd_indicator_count
        COMMENT = 'Patients with eGFR indicating CKD',
    
    -- QRISK Metrics
    COUNT(DISTINCT qrisk.person_id) AS patients_with_qrisk
        COMMENT = 'Patients with QRISK score',
    
    AVG(qrisk.qrisk_score) AS avg_qrisk
        COMMENT = 'Average QRISK score (%)',
    
    COUNT(DISTINCT CASE WHEN qrisk.is_high_cvd_risk THEN demographics.person_id END) AS high_cvd_risk_count
        COMMENT = 'Patients with QRISK ≥10%',
    
    COUNT(DISTINCT CASE WHEN qrisk.is_very_high_cvd_risk THEN demographics.person_id END) AS very_high_cvd_risk_count
        COMMENT = 'Patients with QRISK ≥20%',
    
    -- Urine ACR Metrics
    COUNT(DISTINCT acr.person_id) AS patients_with_acr
        COMMENT = 'Patients with urine ACR measurement',
    
    COUNT(DISTINCT CASE WHEN acr.is_acr_elevated THEN demographics.person_id END) AS elevated_acr_count
        COMMENT = 'Patients with elevated ACR (≥3 mg/mmol)',
    
    COUNT(DISTINCT CASE WHEN acr.is_macroalbuminuria THEN demographics.person_id END) AS macroalbuminuria_count
        COMMENT = 'Patients with macroalbuminuria'
)

COMMENT = 'OLIDS Clinical Observations Semantic View - Latest biomarkers including BP, HbA1c, cholesterol, BMI, eGFR, QRISK, and urine ACR'
