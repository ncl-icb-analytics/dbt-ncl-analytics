{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Population Trends Semantic View
    =====================================
    
    Time-series semantic model for population health trend analysis.
    Built on person_month_analysis_base with 60-month rolling window.
    
    Grain: One row per person per month
    
    Use Cases:
    - Prevalence trends over time (how many people have X condition each month)
    - Incidence trends (new diagnoses per month)
    - Multimorbidity trends
    - Demographic shifts
    - Financial year reporting
    
    Note: This view is for CONDITIONS and DEMOGRAPHICS trends only.
    Clinical observation trends (BP, HbA1c) are methodologically complex
    due to sparse/irregular measurements - use semantic_olids_observations
    for point-in-time biomarker analysis.
#}

TABLES(
    trends AS {{ ref('person_month_analysis_base') }}
        PRIMARY KEY (person_id, analysis_month)
        COMMENT = 'Person-month analysis base with 60-month rolling window. One row per person per month for active registrations.'
)

FACTS(
    -- Condition counts for aggregation
    trends.total_active_conditions COMMENT = 'Total number of active conditions this month',
    trends.total_new_episodes_this_month COMMENT = 'Total new condition episodes this month',
    
    -- Age for averaging
    trends.age COMMENT = 'Age at this analysis month'
)

DIMENSIONS(
    -- Time Dimensions
    trends.analysis_month COMMENT = 'Month end date for analysis period',
    trends.year_number COMMENT = 'Calendar year (e.g., 2024)',
    trends.month_number COMMENT = 'Calendar month (1-12)',
    trends.quarter_number COMMENT = 'Calendar quarter (1-4)',
    trends.month_year_label COMMENT = 'Readable month-year (e.g., MAR 2024)',
    trends.financial_year COMMENT = 'UK financial year (e.g., 2023/24)',
    trends.financial_year_start COMMENT = 'Starting year of financial year',
    trends.financial_quarter COMMENT = 'UK financial quarter (Q1=Apr-Jun)',
    trends.financial_quarter_number COMMENT = 'Financial quarter number (1-4)',
    
    -- Demographics
    trends.gender COMMENT = 'Patient gender',
    trends.age_band_5y COMMENT = '5-year age bands',
    trends.age_band_10y COMMENT = '10-year age bands',
    trends.age_band_nhs COMMENT = 'NHS standard age bands',
    trends.age_band_ons COMMENT = 'ONS standard age bands',
    trends.age_life_stage COMMENT = 'Life stage classification',
    
    -- Ethnicity
    trends.ethnicity_category COMMENT = 'Ethnicity category',
    trends.ethnicity_subcategory COMMENT = 'Ethnicity subcategory',
    trends.ethnicity_granular COMMENT = 'Granular ethnicity',
    
    -- Organisation
    trends.practice_code COMMENT = 'GP practice ODS code',
    trends.practice_name COMMENT = 'GP practice name',
    trends.pcn_code COMMENT = 'Primary Care Network code',
    trends.pcn_name COMMENT = 'Primary Care Network name',
    trends.pcn_name_with_borough COMMENT = 'PCN name with borough prefix',
    trends.borough_registered COMMENT = 'Borough where practice is located',
    trends.neighbourhood_registered COMMENT = 'NCL neighbourhood (registration)',
    
    -- Geography
    trends.lsoa_code_21 COMMENT = 'LSOA 2021 code',
    trends.ward_code COMMENT = 'Electoral ward code',
    trends.ward_name COMMENT = 'Electoral ward name',
    trends.neighbourhood_resident COMMENT = 'NCL neighbourhood (residence)',
    
    -- Deprivation
    trends.imd_decile_19 COMMENT = 'IMD 2019 decile (1=most deprived)',
    trends.imd_quintile_19 COMMENT = 'IMD 2019 quintile',
    trends.imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived)',
    trends.imd_quintile_25 COMMENT = 'IMD 2025 quintile',
    
    -- Registration Status
    trends.is_active COMMENT = 'Currently registered this month',
    trends.is_deceased COMMENT = 'Deceased by this month',
    
    -- Condition Prevalence Flags (has_* = on register this month)
    trends.has_ast COMMENT = 'Has active asthma this month',
    trends.has_copd COMMENT = 'Has active COPD this month',
    trends.has_htn COMMENT = 'Has active hypertension this month',
    trends.has_chd COMMENT = 'Has active CHD this month',
    trends.has_af COMMENT = 'Has active atrial fibrillation this month',
    trends.has_hf COMMENT = 'Has active heart failure this month',
    trends.has_pad COMMENT = 'Has active PAD this month',
    trends.has_dm COMMENT = 'Has active diabetes this month',
    trends.has_gestdiab COMMENT = 'Has active gestational diabetes this month',
    trends.has_ndh COMMENT = 'Has active non-diabetic hyperglycaemia this month',
    trends.has_dep COMMENT = 'Has active depression this month',
    trends.has_smi COMMENT = 'Has active SMI this month',
    trends.has_ckd COMMENT = 'Has active CKD this month',
    trends.has_dem COMMENT = 'Has active dementia this month',
    trends.has_ep COMMENT = 'Has active epilepsy this month',
    trends.has_stia COMMENT = 'Has active stroke/TIA this month',
    trends.has_can COMMENT = 'Has active cancer this month',
    trends.has_pc COMMENT = 'On palliative care this month',
    trends.has_ld COMMENT = 'Has learning disability this month',
    trends.has_frail COMMENT = 'Has frailty this month',
    trends.has_ra COMMENT = 'Has rheumatoid arthritis this month',
    trends.has_ost COMMENT = 'Has osteoporosis this month',
    trends.has_nafld COMMENT = 'Has NAFLD this month',
    trends.has_fh COMMENT = 'Has familial hypercholesterolaemia this month',
    
    -- Incidence Flags (new_* = new episode started this month)
    trends.new_ast COMMENT = 'New asthma episode this month',
    trends.new_copd COMMENT = 'New COPD episode this month',
    trends.new_htn COMMENT = 'New hypertension episode this month',
    trends.new_chd COMMENT = 'New CHD episode this month',
    trends.new_af COMMENT = 'New AF episode this month',
    trends.new_hf COMMENT = 'New heart failure episode this month',
    trends.new_pad COMMENT = 'New PAD episode this month',
    trends.new_dm COMMENT = 'New diabetes episode this month',
    trends.new_gestdiab COMMENT = 'New gestational diabetes episode this month',
    trends.new_ndh COMMENT = 'New NDH episode this month',
    trends.new_dep COMMENT = 'New depression episode this month',
    trends.new_smi COMMENT = 'New SMI episode this month',
    trends.new_ckd COMMENT = 'New CKD episode this month',
    trends.new_dem COMMENT = 'New dementia episode this month',
    trends.new_ep COMMENT = 'New epilepsy episode this month',
    trends.new_stia COMMENT = 'New stroke/TIA episode this month',
    trends.new_can COMMENT = 'New cancer episode this month',
    trends.new_pc COMMENT = 'New palliative care episode this month',
    trends.new_ld COMMENT = 'New learning disability episode this month',
    trends.new_frail COMMENT = 'New frailty episode this month',
    trends.new_ra COMMENT = 'New RA episode this month',
    trends.new_ost COMMENT = 'New osteoporosis episode this month',
    trends.new_nafld COMMENT = 'New NAFLD episode this month',
    trends.new_fh COMMENT = 'New FH episode this month',
    
    -- Summary Flags
    trends.has_any_condition COMMENT = 'Has any active condition this month',
    trends.has_any_new_episode COMMENT = 'Has any new episode this month'
)

METRICS(
    -- Population Counts
    COUNT(DISTINCT trends.person_id) AS patient_count
        COMMENT = 'Total patients in period',
    
    -- Prevalence Counts (people WITH condition this month)
    COUNT(DISTINCT CASE WHEN trends.has_dm THEN trends.person_id END) AS diabetes_prevalence
        COMMENT = 'Patients with diabetes this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_htn THEN trends.person_id END) AS hypertension_prevalence
        COMMENT = 'Patients with hypertension this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_copd THEN trends.person_id END) AS copd_prevalence
        COMMENT = 'Patients with COPD this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_ast THEN trends.person_id END) AS asthma_prevalence
        COMMENT = 'Patients with asthma this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_chd THEN trends.person_id END) AS chd_prevalence
        COMMENT = 'Patients with CHD this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_af THEN trends.person_id END) AS af_prevalence
        COMMENT = 'Patients with AF this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_hf THEN trends.person_id END) AS heart_failure_prevalence
        COMMENT = 'Patients with heart failure this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_ckd THEN trends.person_id END) AS ckd_prevalence
        COMMENT = 'Patients with CKD this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_dep THEN trends.person_id END) AS depression_prevalence
        COMMENT = 'Patients with depression this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_smi THEN trends.person_id END) AS smi_prevalence
        COMMENT = 'Patients with SMI this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_dem THEN trends.person_id END) AS dementia_prevalence
        COMMENT = 'Patients with dementia this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_can THEN trends.person_id END) AS cancer_prevalence
        COMMENT = 'Patients with cancer this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_frail THEN trends.person_id END) AS frailty_prevalence
        COMMENT = 'Patients with frailty this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_ld THEN trends.person_id END) AS learning_disability_prevalence
        COMMENT = 'Patients with learning disability this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_ep THEN trends.person_id END) AS epilepsy_prevalence
        COMMENT = 'Patients with epilepsy this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_stia THEN trends.person_id END) AS stroke_tia_prevalence
        COMMENT = 'Patients with stroke/TIA this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_pc THEN trends.person_id END) AS palliative_care_prevalence
        COMMENT = 'Patients on palliative care this month',
    
    -- Incidence Counts (NEW episodes this month)
    COUNT(DISTINCT CASE WHEN trends.new_dm THEN trends.person_id END) AS diabetes_incidence
        COMMENT = 'New diabetes diagnoses this month',
    
    COUNT(DISTINCT CASE WHEN trends.new_htn THEN trends.person_id END) AS hypertension_incidence
        COMMENT = 'New hypertension diagnoses this month',
    
    COUNT(DISTINCT CASE WHEN trends.new_copd THEN trends.person_id END) AS copd_incidence
        COMMENT = 'New COPD diagnoses this month',
    
    COUNT(DISTINCT CASE WHEN trends.new_ast THEN trends.person_id END) AS asthma_incidence
        COMMENT = 'New asthma diagnoses this month',
    
    COUNT(DISTINCT CASE WHEN trends.new_chd THEN trends.person_id END) AS chd_incidence
        COMMENT = 'New CHD diagnoses this month',
    
    COUNT(DISTINCT CASE WHEN trends.new_af THEN trends.person_id END) AS af_incidence
        COMMENT = 'New AF diagnoses this month',
    
    COUNT(DISTINCT CASE WHEN trends.new_ckd THEN trends.person_id END) AS ckd_incidence
        COMMENT = 'New CKD diagnoses this month',
    
    COUNT(DISTINCT CASE WHEN trends.new_dep THEN trends.person_id END) AS depression_incidence
        COMMENT = 'New depression diagnoses this month',
    
    COUNT(DISTINCT CASE WHEN trends.new_smi THEN trends.person_id END) AS smi_incidence
        COMMENT = 'New SMI diagnoses this month',
    
    COUNT(DISTINCT CASE WHEN trends.new_dem THEN trends.person_id END) AS dementia_incidence
        COMMENT = 'New dementia diagnoses this month',
    
    COUNT(DISTINCT CASE WHEN trends.new_can THEN trends.person_id END) AS cancer_incidence
        COMMENT = 'New cancer diagnoses this month',
    
    COUNT(DISTINCT CASE WHEN trends.new_stia THEN trends.person_id END) AS stroke_tia_incidence
        COMMENT = 'New stroke/TIA this month',
    
    -- Multimorbidity Metrics
    AVG(trends.total_active_conditions) AS avg_conditions_per_patient
        COMMENT = 'Average conditions per patient this month',
    
    COUNT(DISTINCT CASE WHEN trends.total_active_conditions >= 2 THEN trends.person_id END) AS multimorbidity_count
        COMMENT = 'Patients with 2+ conditions this month',
    
    COUNT(DISTINCT CASE WHEN trends.total_active_conditions >= 4 THEN trends.person_id END) AS complex_multimorbidity_count
        COMMENT = 'Patients with 4+ conditions this month',
    
    -- Total Incidence
    SUM(trends.total_new_episodes_this_month) AS total_new_episodes
        COMMENT = 'Total new condition episodes this month',
    
    COUNT(DISTINCT CASE WHEN trends.has_any_new_episode THEN trends.person_id END) AS patients_with_new_episode
        COMMENT = 'Patients with any new episode this month',
    
    -- Demographics
    AVG(trends.age) AS average_age
        COMMENT = 'Average age of population this month'
)

COMMENT = 'OLIDS Population Trends Semantic View - 60-month time series for condition prevalence, incidence, and multimorbidity trends. Use for "over time" questions. Grain: person-month.'
