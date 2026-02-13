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
    
    Core Tables (full population coverage):
    - dim_person_demographics: Core demographics, registration, geography
    - dim_person_conditions: Boolean flags for all conditions + summary counts
    - dim_person_status_summary: Vulnerability, risk factors, and data sharing status
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Core patient demographics including registration, geography, and ethnicity. One row per person.',
    
    conditions AS {{ ref('dim_person_conditions') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Boolean flags for all long-term conditions (QOF and non-QOF). One row per person with FALSE for no conditions.',
    
    status AS {{ ref('dim_person_status_summary') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Vulnerability factors, risk behaviours, polypharmacy, and data sharing status. One row per person.'
)

RELATIONSHIPS(
    conditions (person_id) REFERENCES demographics,
    status (person_id) REFERENCES demographics
)

FACTS(
    -- Condition counts
    conditions.total_conditions COMMENT = 'Total number of active conditions',
    conditions.total_qof_conditions COMMENT = 'Number of QOF-registered conditions',
    conditions.total_non_qof_conditions COMMENT = 'Number of non-QOF conditions',
    
    -- Clinical domain counts
    conditions.cardiovascular_conditions COMMENT = 'Count of cardiovascular conditions',
    conditions.respiratory_conditions COMMENT = 'Count of respiratory conditions',
    conditions.mental_health_conditions COMMENT = 'Count of mental health conditions',
    conditions.metabolic_conditions COMMENT = 'Count of metabolic conditions',
    conditions.musculoskeletal_conditions COMMENT = 'Count of musculoskeletal conditions',
    conditions.neurology_conditions COMMENT = 'Count of neurological conditions',
    
    -- Polypharmacy
    status.medication_count COMMENT = 'Number of current medications',
    status.behavioural_risk_count COMMENT = 'Count of behavioural risk factors'
)

DIMENSIONS(
    -- Core Demographics
    demographics.gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age COMMENT = 'Current age in years',
    demographics.age_band_5y COMMENT = '5-year age bands (0-4, 5-9, ..., 85+)',
    demographics.age_band_10y COMMENT = '10-year age bands (0-9, 10-19, ..., 80+)',
    demographics.age_band_nhs COMMENT = 'NHS Digital standard age bands',
    demographics.age_band_ons COMMENT = 'ONS standard age bands',
    demographics.age_life_stage COMMENT = 'Life stage (Infant, Child, Adolescent, Adult, Senior, Elderly)',
    demographics.is_primary_school_age COMMENT = 'Primary school age (Reception to Year 6)',
    demographics.is_secondary_school_age COMMENT = 'Secondary school age (Year 7 to Year 13)',
    
    -- Ethnicity
    demographics.ethnicity_category COMMENT = 'High-level ethnicity category',
    demographics.ethnicity_subcategory COMMENT = 'Ethnicity subcategory',
    demographics.ethnicity_granular COMMENT = 'Detailed ethnicity classification',
    
    -- Language
    demographics.main_language COMMENT = 'Main spoken language',
    demographics.language_type COMMENT = 'Language type classification',
    demographics.interpreter_needed COMMENT = 'Whether interpreter is required',
    
    -- Registration Status
    demographics.is_active COMMENT = 'Currently registered with NCL GP practice',
    demographics.is_deceased COMMENT = 'Deceased status',
    demographics.inactive_reason COMMENT = 'Reason for inactive status if applicable',
    
    -- Practice/Organisation
    demographics.practice_code COMMENT = 'GP practice ODS code',
    demographics.practice_name COMMENT = 'GP practice name',
    demographics.pcn_code COMMENT = 'Primary Care Network code',
    demographics.pcn_name COMMENT = 'Primary Care Network name',
    demographics.pcn_name_with_borough COMMENT = 'PCN name with borough prefix',
    demographics.icb_code COMMENT = 'ICB code (registration-based)',
    demographics.icb_name COMMENT = 'ICB name (registration-based)',
    demographics.borough_registered COMMENT = 'Borough where GP practice is located',
    demographics.neighbourhood_registered COMMENT = 'NCL neighbourhood (registration)',
    
    -- Geography (Residence)
    demographics.lsoa_code_21 COMMENT = 'Lower Super Output Area 2021 code',
    demographics.lsoa_name_21 COMMENT = 'Lower Super Output Area 2021 name',
    demographics.ward_code COMMENT = 'Electoral ward 2025 code',
    demographics.ward_name COMMENT = 'Electoral ward 2025 name',
    demographics.local_authority_code COMMENT = 'Local authority code',
    demographics.local_authority_name COMMENT = 'Local authority name',
    demographics.borough_resident COMMENT = 'London borough of residence',
    demographics.is_london_resident COMMENT = 'Resides in Greater London',
    demographics.london_classification COMMENT = 'NCL, Other London, or Outside London',
    demographics.neighbourhood_resident COMMENT = 'NCL neighbourhood (residence)',
    
    -- Deprivation
    demographics.imd_decile_19 COMMENT = 'IMD 2019 decile (1=most deprived, 10=least)',
    demographics.imd_quintile_19 COMMENT = 'IMD 2019 quintile label',
    demographics.imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least)',
    demographics.imd_quintile_25 COMMENT = 'IMD 2025 quintile label',
    
    -- Cardiovascular Conditions
    conditions.has_hypertension COMMENT = 'On hypertension register',
    conditions.has_coronary_heart_disease COMMENT = 'On CHD register',
    conditions.has_heart_failure COMMENT = 'On heart failure register',
    conditions.has_atrial_fibrillation COMMENT = 'On AF register',
    conditions.has_stroke_tia COMMENT = 'On stroke/TIA register',
    conditions.has_peripheral_arterial_disease COMMENT = 'On PAD register',
    
    -- Metabolic/Endocrine Conditions
    conditions.has_diabetes COMMENT = 'On diabetes register',
    conditions.has_non_diabetic_hyperglycaemia COMMENT = 'Non-diabetic hyperglycaemia',
    conditions.has_gestational_diabetes COMMENT = 'Gestational diabetes',
    conditions.has_chronic_kidney_disease COMMENT = 'On CKD register',
    conditions.has_hypothyroidism COMMENT = 'Hypothyroidism',
    conditions.has_familial_hypercholesterolaemia COMMENT = 'Familial hypercholesterolaemia',
    conditions.has_obesity COMMENT = 'Recorded obesity',
    
    -- Respiratory Conditions
    conditions.has_copd COMMENT = 'On COPD register',
    conditions.has_asthma COMMENT = 'On asthma register',
    
    -- Mental Health Conditions
    conditions.has_depression COMMENT = 'On depression register',
    conditions.has_severe_mental_illness COMMENT = 'On SMI register',
    conditions.has_anxiety COMMENT = 'On anxiety register',
    conditions.has_dementia COMMENT = 'On dementia register',
    
    -- Neurological Conditions
    conditions.has_epilepsy COMMENT = 'On epilepsy register',
    conditions.has_parkinsons COMMENT = 'Parkinsons disease',
    conditions.has_multiple_sclerosis COMMENT = 'Multiple sclerosis',
    conditions.has_mnd COMMENT = 'Motor neurone disease',
    conditions.has_cerebral_palsy COMMENT = 'Cerebral palsy',
    
    -- Neurodevelopmental Conditions
    conditions.has_learning_disability COMMENT = 'On learning disability register',
    conditions.has_autism COMMENT = 'Autism spectrum disorder',
    
    -- Musculoskeletal Conditions
    conditions.has_osteoporosis COMMENT = 'Osteoporosis',
    conditions.has_rheumatoid_arthritis COMMENT = 'Rheumatoid arthritis',
    
    -- Other Conditions
    conditions.has_cancer COMMENT = 'On cancer register',
    conditions.has_frailty COMMENT = 'Recorded frailty',
    conditions.has_palliative_care COMMENT = 'On palliative care register',
    conditions.has_nafld COMMENT = 'Non-alcoholic fatty liver disease',
    
    -- Vulnerability Status
    status.is_care_home_resident COMMENT = 'Residing in care home',
    status.is_nursing_home_resident COMMENT = 'Residing in nursing home',
    status.care_home_type COMMENT = 'Type of care home residence',
    status.is_homeless_or_chip COMMENT = 'Homeless or registered at CHIP practice',
    status.has_homeless_code COMMENT = 'Has recorded homeless SNOMED code',
    status.is_housebound COMMENT = 'Recorded as housebound',
    status.is_carer COMMENT = 'Recorded as unpaid carer',
    status.carer_type COMMENT = 'Type of carer role',
    status.is_looked_after_child COMMENT = 'Looked after child (under 25)',
    status.has_vulnerability_flag COMMENT = 'Has any vulnerability indicator',
    
    -- Smoking & Alcohol
    status.smoking_status COMMENT = 'Current smoking status (Never Smoked, Ex-Smoker, Current Smoker, Unknown)',
    status.alcohol_status COMMENT = 'Current alcohol status',
    status.alcohol_requires_intervention COMMENT = 'Requires alcohol intervention',
    
    -- Polypharmacy
    status.is_polypharmacy_5plus COMMENT = 'Has 5+ current medications',
    status.is_polypharmacy_10plus COMMENT = 'Has 10+ current medications (severe polypharmacy)',
    status.medication_count_band COMMENT = 'Medication count band (0, 1-4, 5-9, 10-14, 15+)',
    
    -- Pregnancy
    status.is_currently_pregnant COMMENT = 'Currently pregnant',
    
    -- Data Sharing
    status.is_type1_opted_out COMMENT = 'Has Type 1 opt-out',
    status.is_allowed_secondary_use COMMENT = 'Allowed for secondary use'
)

METRICS(
    -- Population Counts
    COUNT(DISTINCT demographics.person_id) AS patient_count
        COMMENT = 'Total number of patients',
    
    COUNT(DISTINCT CASE WHEN demographics.is_active THEN demographics.person_id END) AS active_patient_count
        COMMENT = 'Number of currently registered patients',
    
    COUNT(DISTINCT CASE WHEN demographics.is_deceased THEN demographics.person_id END) AS deceased_patient_count
        COMMENT = 'Number of deceased patients',
    
    -- Demographics Metrics
    AVG(demographics.age) AS average_age
        COMMENT = 'Average age of population',
    
    -- Cardiovascular Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_hypertension THEN demographics.person_id END) AS hypertension_count
        COMMENT = 'Patients with hypertension',
    
    COUNT(DISTINCT CASE WHEN conditions.has_coronary_heart_disease THEN demographics.person_id END) AS chd_count
        COMMENT = 'Patients with coronary heart disease',
    
    COUNT(DISTINCT CASE WHEN conditions.has_heart_failure THEN demographics.person_id END) AS heart_failure_count
        COMMENT = 'Patients with heart failure',
    
    COUNT(DISTINCT CASE WHEN conditions.has_atrial_fibrillation THEN demographics.person_id END) AS af_count
        COMMENT = 'Patients with atrial fibrillation',
    
    COUNT(DISTINCT CASE WHEN conditions.has_stroke_tia THEN demographics.person_id END) AS stroke_tia_count
        COMMENT = 'Patients with stroke/TIA',
    
    -- Metabolic Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_diabetes THEN demographics.person_id END) AS diabetes_count
        COMMENT = 'Patients with diabetes',
    
    COUNT(DISTINCT CASE WHEN conditions.has_chronic_kidney_disease THEN demographics.person_id END) AS ckd_count
        COMMENT = 'Patients with CKD',
    
    -- Respiratory Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_copd THEN demographics.person_id END) AS copd_count
        COMMENT = 'Patients with COPD',
    
    COUNT(DISTINCT CASE WHEN conditions.has_asthma THEN demographics.person_id END) AS asthma_count
        COMMENT = 'Patients with asthma',
    
    -- Mental Health Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_depression THEN demographics.person_id END) AS depression_count
        COMMENT = 'Patients with depression',
    
    COUNT(DISTINCT CASE WHEN conditions.has_severe_mental_illness THEN demographics.person_id END) AS smi_count
        COMMENT = 'Patients with severe mental illness',
    
    COUNT(DISTINCT CASE WHEN conditions.has_dementia THEN demographics.person_id END) AS dementia_count
        COMMENT = 'Patients with dementia',
    
    -- Other Conditions
    COUNT(DISTINCT CASE WHEN conditions.has_cancer THEN demographics.person_id END) AS cancer_count
        COMMENT = 'Patients with cancer',
    
    COUNT(DISTINCT CASE WHEN conditions.has_frailty THEN demographics.person_id END) AS frailty_count
        COMMENT = 'Patients with frailty',
    
    COUNT(DISTINCT CASE WHEN conditions.has_learning_disability THEN demographics.person_id END) AS learning_disability_count
        COMMENT = 'Patients with learning disability',
    
    COUNT(DISTINCT CASE WHEN conditions.has_epilepsy THEN demographics.person_id END) AS epilepsy_count
        COMMENT = 'Patients with epilepsy',
    
    COUNT(DISTINCT CASE WHEN conditions.has_palliative_care THEN demographics.person_id END) AS palliative_care_count
        COMMENT = 'Patients on palliative care register',
    
    -- Multimorbidity Metrics
    AVG(conditions.total_conditions) AS avg_conditions_per_patient
        COMMENT = 'Average number of conditions per patient',
    
    COUNT(DISTINCT CASE WHEN conditions.total_conditions >= 2 THEN demographics.person_id END) AS multimorbidity_count
        COMMENT = 'Patients with 2+ conditions (multimorbidity)',
    
    COUNT(DISTINCT CASE WHEN conditions.total_conditions >= 4 THEN demographics.person_id END) AS complex_multimorbidity_count
        COMMENT = 'Patients with 4+ conditions (complex multimorbidity)',
    
    -- Smoking Metrics
    COUNT(DISTINCT CASE WHEN status.smoking_status = 'Current Smoker' THEN demographics.person_id END) AS current_smoker_count
        COMMENT = 'Current smokers',
    
    COUNT(DISTINCT CASE WHEN status.smoking_status = 'Ex-Smoker' THEN demographics.person_id END) AS ex_smoker_count
        COMMENT = 'Ex-smokers',
    
    COUNT(DISTINCT CASE WHEN status.smoking_status = 'Never Smoked' THEN demographics.person_id END) AS never_smoked_count
        COMMENT = 'Never smoked',
    
    -- Alcohol Metrics
    COUNT(DISTINCT CASE WHEN status.alcohol_requires_intervention THEN demographics.person_id END) AS alcohol_intervention_count
        COMMENT = 'Patients requiring alcohol intervention',
    
    -- Vulnerability Metrics
    COUNT(DISTINCT CASE WHEN status.is_care_home_resident THEN demographics.person_id END) AS care_home_resident_count
        COMMENT = 'Care home residents',
    
    COUNT(DISTINCT CASE WHEN status.is_homeless_or_chip THEN demographics.person_id END) AS homeless_count
        COMMENT = 'Homeless or registered at CHIP',
    
    COUNT(DISTINCT CASE WHEN status.is_housebound THEN demographics.person_id END) AS housebound_count
        COMMENT = 'Housebound patients',
    
    COUNT(DISTINCT CASE WHEN status.is_carer THEN demographics.person_id END) AS carer_count
        COMMENT = 'Unpaid carers',
    
    COUNT(DISTINCT CASE WHEN status.is_looked_after_child THEN demographics.person_id END) AS looked_after_child_count
        COMMENT = 'Looked after children',
    
    COUNT(DISTINCT CASE WHEN status.has_vulnerability_flag THEN demographics.person_id END) AS vulnerable_count
        COMMENT = 'Patients with any vulnerability flag',
    
    -- Polypharmacy Metrics
    AVG(status.medication_count) AS avg_medication_count
        COMMENT = 'Average medications per patient',
    
    COUNT(DISTINCT CASE WHEN status.is_polypharmacy_5plus THEN demographics.person_id END) AS polypharmacy_5plus_count
        COMMENT = 'Patients with 5+ medications',
    
    COUNT(DISTINCT CASE WHEN status.is_polypharmacy_10plus THEN demographics.person_id END) AS polypharmacy_10plus_count
        COMMENT = 'Patients with 10+ medications (severe polypharmacy)',
    
    -- Pregnancy Metrics
    COUNT(DISTINCT CASE WHEN status.is_currently_pregnant THEN demographics.person_id END) AS pregnant_count
        COMMENT = 'Currently pregnant patients',
    
    -- Data Sharing Metrics
    COUNT(DISTINCT CASE WHEN status.is_type1_opted_out THEN demographics.person_id END) AS type1_optout_count
        COMMENT = 'Patients with Type 1 opt-out',
    
    COUNT(DISTINCT CASE WHEN status.is_allowed_secondary_use THEN demographics.person_id END) AS secondary_use_allowed_count
        COMMENT = 'Patients allowed for secondary use'
)

COMMENT = 'OLIDS Population Health Semantic View - NCL registered population with demographics, conditions, vulnerability factors, and risk behaviours. Enables natural language queries via Cortex Analyst.'
