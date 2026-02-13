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
        PRIMARY KEY (person_id),
    
    conditions AS {{ ref('dim_person_conditions') }}
        PRIMARY KEY (person_id),
    
    status AS {{ ref('dim_person_status_summary') }}
        PRIMARY KEY (person_id)
)

RELATIONSHIPS(
    conditions (person_id) REFERENCES demographics,
    status (person_id) REFERENCES demographics
)

FACTS(
    demographics.age,
    conditions.total_conditions,
    conditions.total_qof_conditions,
    conditions.total_non_qof_conditions,
    conditions.cardiovascular_conditions,
    conditions.respiratory_conditions,
    conditions.mental_health_conditions,
    conditions.metabolic_conditions,
    conditions.musculoskeletal_conditions,
    conditions.neurology_conditions,
    status.medication_count,
    status.behavioural_risk_count
)

DIMENSIONS(
    -- Core Demographics
    demographics.gender,
    demographics.age_band_5y,
    demographics.age_band_10y,
    demographics.age_band_nhs,
    demographics.age_band_ons,
    demographics.age_life_stage,
    demographics.is_primary_school_age,
    demographics.is_secondary_school_age,
    
    -- Ethnicity
    demographics.ethnicity_category,
    demographics.ethnicity_subcategory,
    demographics.ethnicity_granular,
    
    -- Language
    demographics.main_language,
    demographics.language_type,
    demographics.interpreter_needed,
    
    -- Registration Status
    demographics.is_active,
    demographics.is_deceased,
    demographics.inactive_reason,
    
    -- Practice/Organisation
    demographics.practice_code,
    demographics.practice_name,
    demographics.pcn_code,
    demographics.pcn_name,
    demographics.pcn_name_with_borough,
    demographics.icb_code,
    demographics.icb_name,
    demographics.borough_registered,
    demographics.neighbourhood_registered,
    
    -- Geography (Residence)
    demographics.lsoa_code_21,
    demographics.lsoa_name_21,
    demographics.ward_code,
    demographics.ward_name,
    demographics.local_authority_code,
    demographics.local_authority_name,
    demographics.borough_resident,
    demographics.is_london_resident,
    demographics.london_classification,
    demographics.neighbourhood_resident,
    
    -- Deprivation
    demographics.imd_decile_19,
    demographics.imd_quintile_19,
    demographics.imd_decile_25,
    demographics.imd_quintile_25,
    
    -- Cardiovascular Conditions
    conditions.has_hypertension,
    conditions.has_coronary_heart_disease,
    conditions.has_heart_failure,
    conditions.has_atrial_fibrillation,
    conditions.has_stroke_tia,
    conditions.has_peripheral_arterial_disease,
    
    -- Metabolic/Endocrine Conditions
    conditions.has_diabetes,
    conditions.has_non_diabetic_hyperglycaemia,
    conditions.has_gestational_diabetes,
    conditions.has_chronic_kidney_disease,
    conditions.has_hypothyroidism,
    conditions.has_familial_hypercholesterolaemia,
    conditions.has_obesity,
    
    -- Respiratory Conditions
    conditions.has_copd,
    conditions.has_asthma,
    
    -- Mental Health Conditions
    conditions.has_depression,
    conditions.has_severe_mental_illness,
    conditions.has_anxiety,
    conditions.has_dementia,
    
    -- Neurological Conditions
    conditions.has_epilepsy,
    conditions.has_parkinsons,
    conditions.has_multiple_sclerosis,
    conditions.has_mnd,
    conditions.has_cerebral_palsy,
    
    -- Neurodevelopmental Conditions
    conditions.has_learning_disability,
    conditions.has_autism,
    
    -- Musculoskeletal Conditions
    conditions.has_osteoporosis,
    conditions.has_rheumatoid_arthritis,
    
    -- Other Conditions
    conditions.has_cancer,
    conditions.has_frailty,
    conditions.has_palliative_care,
    conditions.has_nafld,
    
    -- Vulnerability Status
    status.is_care_home_resident,
    status.is_nursing_home_resident,
    status.care_home_type,
    status.is_homeless_or_chip,
    status.has_homeless_code,
    status.is_housebound,
    status.is_carer,
    status.carer_type,
    status.is_looked_after_child,
    status.has_vulnerability_flag,
    
    -- Smoking & Alcohol
    status.smoking_status,
    status.alcohol_status,
    status.alcohol_requires_intervention,
    
    -- Polypharmacy
    status.is_polypharmacy_5plus,
    status.is_polypharmacy_10plus,
    status.medication_count_band,
    
    -- Pregnancy
    status.is_currently_pregnant,
    
    -- Data Sharing
    status.is_type1_opted_out,
    status.is_allowed_secondary_use
)

METRICS(
    -- Population Counts
    COUNT(DISTINCT demographics.person_id) AS patient_count,
    COUNT(DISTINCT CASE WHEN demographics.is_active THEN demographics.person_id END) AS active_patient_count,
    COUNT(DISTINCT CASE WHEN demographics.is_deceased THEN demographics.person_id END) AS deceased_patient_count,
    
    -- Demographics Metrics
    AVG(demographics.age) AS average_age,
    
    -- Cardiovascular Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_hypertension THEN demographics.person_id END) AS hypertension_count,
    COUNT(DISTINCT CASE WHEN conditions.has_coronary_heart_disease THEN demographics.person_id END) AS chd_count,
    COUNT(DISTINCT CASE WHEN conditions.has_heart_failure THEN demographics.person_id END) AS heart_failure_count,
    COUNT(DISTINCT CASE WHEN conditions.has_atrial_fibrillation THEN demographics.person_id END) AS af_count,
    COUNT(DISTINCT CASE WHEN conditions.has_stroke_tia THEN demographics.person_id END) AS stroke_tia_count,
    
    -- Metabolic Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_diabetes THEN demographics.person_id END) AS diabetes_count,
    COUNT(DISTINCT CASE WHEN conditions.has_chronic_kidney_disease THEN demographics.person_id END) AS ckd_count,
    
    -- Respiratory Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_copd THEN demographics.person_id END) AS copd_count,
    COUNT(DISTINCT CASE WHEN conditions.has_asthma THEN demographics.person_id END) AS asthma_count,
    
    -- Mental Health Prevalence
    COUNT(DISTINCT CASE WHEN conditions.has_depression THEN demographics.person_id END) AS depression_count,
    COUNT(DISTINCT CASE WHEN conditions.has_severe_mental_illness THEN demographics.person_id END) AS smi_count,
    COUNT(DISTINCT CASE WHEN conditions.has_dementia THEN demographics.person_id END) AS dementia_count,
    
    -- Other Conditions
    COUNT(DISTINCT CASE WHEN conditions.has_cancer THEN demographics.person_id END) AS cancer_count,
    COUNT(DISTINCT CASE WHEN conditions.has_frailty THEN demographics.person_id END) AS frailty_count,
    COUNT(DISTINCT CASE WHEN conditions.has_learning_disability THEN demographics.person_id END) AS learning_disability_count,
    COUNT(DISTINCT CASE WHEN conditions.has_epilepsy THEN demographics.person_id END) AS epilepsy_count,
    COUNT(DISTINCT CASE WHEN conditions.has_palliative_care THEN demographics.person_id END) AS palliative_care_count,
    
    -- Multimorbidity Metrics
    AVG(conditions.total_conditions) AS avg_conditions_per_patient,
    COUNT(DISTINCT CASE WHEN conditions.total_conditions >= 2 THEN demographics.person_id END) AS multimorbidity_count,
    COUNT(DISTINCT CASE WHEN conditions.total_conditions >= 4 THEN demographics.person_id END) AS complex_multimorbidity_count,
    
    -- Smoking Metrics
    COUNT(DISTINCT CASE WHEN status.smoking_status = 'Current Smoker' THEN demographics.person_id END) AS current_smoker_count,
    COUNT(DISTINCT CASE WHEN status.smoking_status = 'Ex-Smoker' THEN demographics.person_id END) AS ex_smoker_count,
    COUNT(DISTINCT CASE WHEN status.smoking_status = 'Never Smoked' THEN demographics.person_id END) AS never_smoked_count,
    
    -- Alcohol Metrics
    COUNT(DISTINCT CASE WHEN status.alcohol_requires_intervention THEN demographics.person_id END) AS alcohol_intervention_count,
    
    -- Vulnerability Metrics
    COUNT(DISTINCT CASE WHEN status.is_care_home_resident THEN demographics.person_id END) AS care_home_resident_count,
    COUNT(DISTINCT CASE WHEN status.is_homeless_or_chip THEN demographics.person_id END) AS homeless_count,
    COUNT(DISTINCT CASE WHEN status.is_housebound THEN demographics.person_id END) AS housebound_count,
    COUNT(DISTINCT CASE WHEN status.is_carer THEN demographics.person_id END) AS carer_count,
    COUNT(DISTINCT CASE WHEN status.is_looked_after_child THEN demographics.person_id END) AS looked_after_child_count,
    COUNT(DISTINCT CASE WHEN status.has_vulnerability_flag THEN demographics.person_id END) AS vulnerable_count,
    
    -- Polypharmacy Metrics
    AVG(status.medication_count) AS avg_medication_count,
    COUNT(DISTINCT CASE WHEN status.is_polypharmacy_5plus THEN demographics.person_id END) AS polypharmacy_5plus_count,
    COUNT(DISTINCT CASE WHEN status.is_polypharmacy_10plus THEN demographics.person_id END) AS polypharmacy_10plus_count,
    
    -- Pregnancy Metrics
    COUNT(DISTINCT CASE WHEN status.is_currently_pregnant THEN demographics.person_id END) AS pregnant_count,
    
    -- Data Sharing Metrics
    COUNT(DISTINCT CASE WHEN status.is_type1_opted_out THEN demographics.person_id END) AS type1_optout_count,
    COUNT(DISTINCT CASE WHEN status.is_allowed_secondary_use THEN demographics.person_id END) AS secondary_use_allowed_count
)

COMMENT = 'OLIDS Population Health Semantic View - NCL registered population with demographics, conditions, vulnerability factors, and risk behaviours. Enables natural language queries via Cortex Analyst.'
