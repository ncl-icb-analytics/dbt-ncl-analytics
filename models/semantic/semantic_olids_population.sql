{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Population Semantic View
    ==============================
    
    Comprehensive semantic model for NCL population health analysis.
    Combines demographics, conditions, and person status into a unified
    semantic layer for Cortex Analyst and governed metric definitions.
    
    Grain: One row per person (current state)
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Core patient demographics including registration, geography, and ethnicity',
    
    conditions AS {{ ref('dim_person_conditions') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Boolean flags for all long-term conditions with summary counts',
    
    status AS {{ ref('dim_person_status_summary') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Vulnerability factors, polypharmacy, smoking, alcohol, and data sharing status'
)

RELATIONSHIPS(
    conditions (person_id) REFERENCES demographics,
    status (person_id) REFERENCES demographics
)

FACTS(
    demographics.age AS age COMMENT = 'Current age in years',
    conditions.total_conditions AS total_conditions COMMENT = 'Total number of active conditions',
    conditions.total_qof_conditions AS total_qof_conditions COMMENT = 'Number of QOF-registered conditions',
    conditions.total_non_qof_conditions AS total_non_qof_conditions COMMENT = 'Number of non-QOF conditions',
    conditions.cardiovascular_conditions AS cardiovascular_conditions COMMENT = 'Count of cardiovascular conditions',
    conditions.respiratory_conditions AS respiratory_conditions COMMENT = 'Count of respiratory conditions',
    conditions.mental_health_conditions AS mental_health_conditions COMMENT = 'Count of mental health conditions',
    conditions.metabolic_conditions AS metabolic_conditions COMMENT = 'Count of metabolic conditions',
    status.medication_count AS medication_count COMMENT = 'Number of current medications',
    status.behavioural_risk_count AS behavioural_risk_count COMMENT = 'Count of behavioural risk factors'
)

DIMENSIONS(
    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands (0-4, 5-9, ..., 85+)',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS Digital standard age bands',
    demographics.age_life_stage AS age_life_stage COMMENT = 'Life stage (Infant, Child, Adolescent, Adult, Senior, Elderly)',
    
    -- Ethnicity
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'High-level ethnicity category',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory',
    demographics.ethnicity_granular AS ethnicity_granular COMMENT = 'Detailed ethnicity classification',
    
    -- Language
    demographics.main_language AS main_language COMMENT = 'Main spoken language',
    demographics.interpreter_needed AS interpreter_needed COMMENT = 'Whether interpreter is required',
    
    -- Registration Status
    demographics.is_active AS is_active COMMENT = 'Currently registered with NCL GP practice',
    demographics.is_deceased AS is_deceased COMMENT = 'Deceased status',
    
    -- Practice/Organisation
    demographics.practice_code AS practice_code COMMENT = 'GP practice ODS code',
    demographics.practice_name AS practice_name COMMENT = 'GP practice name',
    demographics.pcn_code AS pcn_code COMMENT = 'Primary Care Network code',
    demographics.pcn_name AS pcn_name COMMENT = 'Primary Care Network name',
    demographics.pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'PCN name with borough prefix',
    demographics.borough_registered AS borough_registered COMMENT = 'Borough where GP practice is located',
    demographics.neighbourhood_registered AS neighbourhood_registered COMMENT = 'NCL neighbourhood based on registration',
    
    -- Geography (Residence)
    demographics.lsoa_code_21 AS lsoa_code_21 COMMENT = 'Lower Super Output Area 2021 code',
    demographics.ward_code AS ward_code COMMENT = 'Electoral ward 2025 code',
    demographics.ward_name AS ward_name COMMENT = 'Electoral ward 2025 name',
    demographics.borough_resident AS borough_resident COMMENT = 'London borough of residence',
    demographics.is_london_resident AS is_london_resident COMMENT = 'Resides in Greater London',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'NCL neighbourhood based on residence',
    
    -- Deprivation
    demographics.imd_decile_19 AS imd_decile_19 COMMENT = 'IMD 2019 decile (1=most deprived, 10=least)',
    demographics.imd_quintile_19 AS imd_quintile_19 COMMENT = 'IMD 2019 quintile label',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least)',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile label',
    
    -- Cardiovascular Conditions
    conditions.has_hypertension AS has_hypertension WITH SYNONYMS = ('HTN', 'high blood pressure') COMMENT = 'On hypertension register',
    conditions.has_coronary_heart_disease AS has_coronary_heart_disease WITH SYNONYMS = ('CHD', 'IHD', 'ischaemic heart disease') COMMENT = 'On CHD register',
    conditions.has_heart_failure AS has_heart_failure WITH SYNONYMS = ('HF') COMMENT = 'On heart failure register',
    conditions.has_atrial_fibrillation AS has_atrial_fibrillation WITH SYNONYMS = ('AF', 'AFib') COMMENT = 'On AF register',
    conditions.has_stroke_tia AS has_stroke_tia WITH SYNONYMS = ('stroke', 'TIA', 'CVA') COMMENT = 'On stroke/TIA register',
    conditions.has_peripheral_arterial_disease AS has_peripheral_arterial_disease WITH SYNONYMS = ('PAD', 'PVD') COMMENT = 'On PAD register',
    
    -- Metabolic/Endocrine Conditions
    conditions.has_diabetes AS has_diabetes WITH SYNONYMS = ('DM', 'diabetic', 'T1DM', 'T2DM') COMMENT = 'On diabetes register',
    conditions.has_non_diabetic_hyperglycaemia AS has_non_diabetic_hyperglycaemia WITH SYNONYMS = ('NDH', 'prediabetes') COMMENT = 'Non-diabetic hyperglycaemia',
    conditions.has_chronic_kidney_disease AS has_chronic_kidney_disease WITH SYNONYMS = ('CKD', 'renal disease') COMMENT = 'On CKD register',
    conditions.has_obesity AS has_obesity COMMENT = 'Recorded obesity',
    
    -- Respiratory Conditions
    conditions.has_copd AS has_copd WITH SYNONYMS = ('COPD', 'emphysema', 'chronic bronchitis') COMMENT = 'On COPD register',
    conditions.has_asthma AS has_asthma COMMENT = 'On asthma register',
    
    -- Mental Health Conditions
    conditions.has_depression AS has_depression COMMENT = 'On depression register',
    conditions.has_severe_mental_illness AS has_severe_mental_illness WITH SYNONYMS = ('SMI', 'schizophrenia', 'bipolar') COMMENT = 'On SMI register',
    conditions.has_anxiety AS has_anxiety COMMENT = 'On anxiety register',
    conditions.has_dementia AS has_dementia COMMENT = 'On dementia register',
    
    -- Neurological Conditions
    conditions.has_epilepsy AS has_epilepsy COMMENT = 'On epilepsy register',
    conditions.has_learning_disability AS has_learning_disability WITH SYNONYMS = ('LD', 'learning difficulties') COMMENT = 'On learning disability register',
    
    -- Other Conditions
    conditions.has_cancer AS has_cancer COMMENT = 'On cancer register',
    conditions.has_frailty AS has_frailty COMMENT = 'Recorded frailty',
    conditions.has_palliative_care AS has_palliative_care COMMENT = 'On palliative care register',
    
    -- Vulnerability Status
    status.is_care_home_resident AS is_care_home_resident COMMENT = 'Residing in care home',
    status.is_nursing_home_resident AS is_nursing_home_resident COMMENT = 'Residing in nursing home',
    status.care_home_type AS care_home_type COMMENT = 'Type of care home residence',
    status.is_homeless_or_chip AS is_homeless_or_chip WITH SYNONYMS = ('homeless', 'rough sleeper', 'NFA') COMMENT = 'Homeless or registered at CHIP practice',
    status.is_housebound AS is_housebound COMMENT = 'Recorded as housebound',
    status.is_carer AS is_carer WITH SYNONYMS = ('unpaid carer', 'caregiver') COMMENT = 'Recorded as unpaid carer',
    status.is_looked_after_child AS is_looked_after_child WITH SYNONYMS = ('LAC', 'looked after', 'in care') COMMENT = 'Looked after child (under 25)',
    status.has_vulnerability_flag AS has_vulnerability_flag COMMENT = 'Has any vulnerability indicator',
    
    -- Smoking & Alcohol
    status.smoking_status AS smoking_status COMMENT = 'Current smoking status (Never Smoked, Ex-Smoker, Current Smoker, Unknown)',
    status.alcohol_status AS alcohol_status COMMENT = 'Current alcohol status',
    status.alcohol_requires_intervention AS alcohol_requires_intervention COMMENT = 'Requires alcohol intervention',
    
    -- Polypharmacy
    status.is_polypharmacy_5plus AS is_polypharmacy_5plus COMMENT = 'Has 5+ current medications',
    status.is_polypharmacy_10plus AS is_polypharmacy_10plus COMMENT = 'Has 10+ current medications (severe polypharmacy)',
    status.medication_count_band AS medication_count_band COMMENT = 'Medication count band (0, 1-4, 5-9, 10-14, 15+)',
    
    -- Pregnancy
    status.is_currently_pregnant AS is_currently_pregnant COMMENT = 'Currently pregnant',
    
    -- Data Sharing
    status.is_type1_opted_out AS is_type1_opted_out COMMENT = 'Has Type 1 opt-out',
    status.is_allowed_secondary_use AS is_allowed_secondary_use COMMENT = 'Allowed for secondary use'
)

METRICS(
    -- Population Counts
    COUNT(DISTINCT demographics.person_id) AS patient_count COMMENT = 'Total number of patients',
    COUNT(DISTINCT CASE WHEN demographics.is_active THEN demographics.person_id END) AS active_patient_count COMMENT = 'Currently registered patients',
    COUNT(DISTINCT CASE WHEN demographics.is_deceased THEN demographics.person_id END) AS deceased_patient_count COMMENT = 'Deceased patients',
    
    -- Demographics
    AVG(demographics.age) AS average_age COMMENT = 'Average age of population',
    
    -- Cardiovascular Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_hypertension THEN demographics.person_id END) AS hypertension_count COMMENT = 'Patients with hypertension',
    COUNT(DISTINCT CASE WHEN conditions.has_coronary_heart_disease THEN demographics.person_id END) AS chd_count COMMENT = 'Patients with CHD',
    COUNT(DISTINCT CASE WHEN conditions.has_heart_failure THEN demographics.person_id END) AS heart_failure_count COMMENT = 'Patients with heart failure',
    COUNT(DISTINCT CASE WHEN conditions.has_atrial_fibrillation THEN demographics.person_id END) AS af_count COMMENT = 'Patients with AF',
    COUNT(DISTINCT CASE WHEN conditions.has_stroke_tia THEN demographics.person_id END) AS stroke_tia_count COMMENT = 'Patients with stroke/TIA',
    COUNT(DISTINCT CASE WHEN conditions.has_peripheral_arterial_disease THEN demographics.person_id END) AS pad_count COMMENT = 'Patients with PAD',
    
    -- Metabolic Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_diabetes THEN demographics.person_id END) AS diabetes_count COMMENT = 'Patients with diabetes',
    COUNT(DISTINCT CASE WHEN conditions.has_chronic_kidney_disease THEN demographics.person_id END) AS ckd_count COMMENT = 'Patients with CKD',
    
    -- Respiratory Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_copd THEN demographics.person_id END) AS copd_count COMMENT = 'Patients with COPD',
    COUNT(DISTINCT CASE WHEN conditions.has_asthma THEN demographics.person_id END) AS asthma_count COMMENT = 'Patients with asthma',
    
    -- Mental Health Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_depression THEN demographics.person_id END) AS depression_count COMMENT = 'Patients with depression',
    COUNT(DISTINCT CASE WHEN conditions.has_severe_mental_illness THEN demographics.person_id END) AS smi_count COMMENT = 'Patients with SMI',
    COUNT(DISTINCT CASE WHEN conditions.has_dementia THEN demographics.person_id END) AS dementia_count COMMENT = 'Patients with dementia',
    COUNT(DISTINCT CASE WHEN conditions.has_anxiety THEN demographics.person_id END) AS anxiety_count COMMENT = 'Patients with anxiety',
    
    -- Other Conditions
    COUNT(DISTINCT CASE WHEN conditions.has_cancer THEN demographics.person_id END) AS cancer_count COMMENT = 'Patients with cancer',
    COUNT(DISTINCT CASE WHEN conditions.has_frailty THEN demographics.person_id END) AS frailty_count COMMENT = 'Patients with frailty',
    COUNT(DISTINCT CASE WHEN conditions.has_learning_disability THEN demographics.person_id END) AS learning_disability_count COMMENT = 'Patients with LD',
    COUNT(DISTINCT CASE WHEN conditions.has_epilepsy THEN demographics.person_id END) AS epilepsy_count COMMENT = 'Patients with epilepsy',
    COUNT(DISTINCT CASE WHEN conditions.has_palliative_care THEN demographics.person_id END) AS palliative_care_count COMMENT = 'Patients on palliative care',
    
    -- Multimorbidity
    AVG(conditions.total_conditions) AS avg_conditions_per_patient COMMENT = 'Average conditions per patient',
    COUNT(DISTINCT CASE WHEN conditions.total_conditions >= 2 THEN demographics.person_id END) AS multimorbidity_count COMMENT = 'Patients with 2+ conditions',
    COUNT(DISTINCT CASE WHEN conditions.total_conditions >= 4 THEN demographics.person_id END) AS complex_multimorbidity_count COMMENT = 'Patients with 4+ conditions',
    
    -- Smoking
    COUNT(DISTINCT CASE WHEN status.smoking_status = 'Current Smoker' THEN demographics.person_id END) AS current_smoker_count COMMENT = 'Current smokers',
    COUNT(DISTINCT CASE WHEN status.smoking_status = 'Ex-Smoker' THEN demographics.person_id END) AS ex_smoker_count COMMENT = 'Ex-smokers',
    
    -- Vulnerability
    COUNT(DISTINCT CASE WHEN status.is_care_home_resident THEN demographics.person_id END) AS care_home_resident_count COMMENT = 'Care home residents',
    COUNT(DISTINCT CASE WHEN status.is_homeless_or_chip THEN demographics.person_id END) AS homeless_count COMMENT = 'Homeless patients',
    COUNT(DISTINCT CASE WHEN status.is_housebound THEN demographics.person_id END) AS housebound_count COMMENT = 'Housebound patients',
    COUNT(DISTINCT CASE WHEN status.is_carer THEN demographics.person_id END) AS carer_count COMMENT = 'Unpaid carers',
    COUNT(DISTINCT CASE WHEN status.has_vulnerability_flag THEN demographics.person_id END) AS vulnerable_count COMMENT = 'Patients with any vulnerability',
    
    -- Polypharmacy
    AVG(status.medication_count) AS avg_medication_count COMMENT = 'Average medications per patient',
    COUNT(DISTINCT CASE WHEN status.is_polypharmacy_5plus THEN demographics.person_id END) AS polypharmacy_5plus_count COMMENT = 'Patients with 5+ medications',
    COUNT(DISTINCT CASE WHEN status.is_polypharmacy_10plus THEN demographics.person_id END) AS polypharmacy_10plus_count COMMENT = 'Patients with 10+ medications'
)

COMMENT = 'OLIDS Population Health Semantic View - NCL registered population with demographics, conditions, vulnerability factors, and risk behaviours. Grain: one row per person (current state).'
AI_SQL_GENERATION 'Always filter to is_active = TRUE unless the user explicitly asks about deceased or inactive patients. Use borough_registered for practice-based geography and borough_resident for residence-based geography. IMD 2025 (imd_decile_25, imd_quintile_25) is preferred over IMD 2019.'
AI_QUESTION_CATEGORIZATION 'Use this view for questions about: condition prevalence, demographics, multimorbidity, vulnerability, smoking, polypharmacy, and population counts. For clinical biomarkers (BP, HbA1c, BMI, cholesterol) use semantic_olids_observations. For trends over time use semantic_olids_trends.'
