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
)

FACTS(
    trends.total_active_conditions,
    trends.total_new_episodes_this_month,
    trends.age
)

DIMENSIONS(
    -- Time Dimensions
    trends.analysis_month,
    trends.year_number,
    trends.month_number,
    trends.quarter_number,
    trends.month_year_label,
    trends.financial_year,
    trends.financial_year_start,
    trends.financial_quarter,
    trends.financial_quarter_number,
    
    -- Demographics
    trends.gender,
    trends.age_band_5y,
    trends.age_band_10y,
    trends.age_band_nhs,
    trends.age_band_ons,
    trends.age_life_stage,
    
    -- Ethnicity
    trends.ethnicity_category,
    trends.ethnicity_subcategory,
    trends.ethnicity_granular,
    
    -- Organisation
    trends.practice_code,
    trends.practice_name,
    trends.pcn_code,
    trends.pcn_name,
    trends.pcn_name_with_borough,
    trends.borough_registered,
    trends.neighbourhood_registered,
    
    -- Geography
    trends.lsoa_code_21,
    trends.ward_code,
    trends.ward_name,
    trends.neighbourhood_resident,
    
    -- Deprivation
    trends.imd_decile_19,
    trends.imd_quintile_19,
    trends.imd_decile_25,
    trends.imd_quintile_25,
    
    -- Registration Status
    trends.is_active,
    trends.is_deceased,
    
    -- Condition Prevalence Flags (has_* = on register this month)
    trends.has_ast,
    trends.has_copd,
    trends.has_htn,
    trends.has_chd,
    trends.has_af,
    trends.has_hf,
    trends.has_pad,
    trends.has_dm,
    trends.has_gestdiab,
    trends.has_ndh,
    trends.has_dep,
    trends.has_smi,
    trends.has_ckd,
    trends.has_dem,
    trends.has_ep,
    trends.has_stia,
    trends.has_can,
    trends.has_pc,
    trends.has_ld,
    trends.has_frail,
    trends.has_ra,
    trends.has_ost,
    trends.has_nafld,
    trends.has_fh,
    
    -- Incidence Flags (new_* = new episode started this month)
    trends.new_ast,
    trends.new_copd,
    trends.new_htn,
    trends.new_chd,
    trends.new_af,
    trends.new_hf,
    trends.new_pad,
    trends.new_dm,
    trends.new_gestdiab,
    trends.new_ndh,
    trends.new_dep,
    trends.new_smi,
    trends.new_ckd,
    trends.new_dem,
    trends.new_ep,
    trends.new_stia,
    trends.new_can,
    trends.new_pc,
    trends.new_ld,
    trends.new_frail,
    trends.new_ra,
    trends.new_ost,
    trends.new_nafld,
    trends.new_fh,
    
    -- Summary Flags
    trends.has_any_condition,
    trends.has_any_new_episode
)

METRICS(
    -- Population Counts
    COUNT(DISTINCT trends.person_id) AS patient_count,
    
    -- Prevalence Counts (people WITH condition this month)
    COUNT(DISTINCT CASE WHEN trends.has_dm THEN trends.person_id END) AS diabetes_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_htn THEN trends.person_id END) AS hypertension_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_copd THEN trends.person_id END) AS copd_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_ast THEN trends.person_id END) AS asthma_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_chd THEN trends.person_id END) AS chd_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_af THEN trends.person_id END) AS af_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_hf THEN trends.person_id END) AS heart_failure_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_ckd THEN trends.person_id END) AS ckd_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_dep THEN trends.person_id END) AS depression_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_smi THEN trends.person_id END) AS smi_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_dem THEN trends.person_id END) AS dementia_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_can THEN trends.person_id END) AS cancer_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_frail THEN trends.person_id END) AS frailty_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_ld THEN trends.person_id END) AS learning_disability_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_ep THEN trends.person_id END) AS epilepsy_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_stia THEN trends.person_id END) AS stroke_tia_prevalence,
    COUNT(DISTINCT CASE WHEN trends.has_pc THEN trends.person_id END) AS palliative_care_prevalence,
    
    -- Incidence Counts (NEW episodes this month)
    COUNT(DISTINCT CASE WHEN trends.new_dm THEN trends.person_id END) AS diabetes_incidence,
    COUNT(DISTINCT CASE WHEN trends.new_htn THEN trends.person_id END) AS hypertension_incidence,
    COUNT(DISTINCT CASE WHEN trends.new_copd THEN trends.person_id END) AS copd_incidence,
    COUNT(DISTINCT CASE WHEN trends.new_ast THEN trends.person_id END) AS asthma_incidence,
    COUNT(DISTINCT CASE WHEN trends.new_chd THEN trends.person_id END) AS chd_incidence,
    COUNT(DISTINCT CASE WHEN trends.new_af THEN trends.person_id END) AS af_incidence,
    COUNT(DISTINCT CASE WHEN trends.new_ckd THEN trends.person_id END) AS ckd_incidence,
    COUNT(DISTINCT CASE WHEN trends.new_dep THEN trends.person_id END) AS depression_incidence,
    COUNT(DISTINCT CASE WHEN trends.new_smi THEN trends.person_id END) AS smi_incidence,
    COUNT(DISTINCT CASE WHEN trends.new_dem THEN trends.person_id END) AS dementia_incidence,
    COUNT(DISTINCT CASE WHEN trends.new_can THEN trends.person_id END) AS cancer_incidence,
    COUNT(DISTINCT CASE WHEN trends.new_stia THEN trends.person_id END) AS stroke_tia_incidence,
    
    -- Multimorbidity Metrics
    AVG(trends.total_active_conditions) AS avg_conditions_per_patient,
    COUNT(DISTINCT CASE WHEN trends.total_active_conditions >= 2 THEN trends.person_id END) AS multimorbidity_count,
    COUNT(DISTINCT CASE WHEN trends.total_active_conditions >= 4 THEN trends.person_id END) AS complex_multimorbidity_count,
    
    -- Total Incidence
    SUM(trends.total_new_episodes_this_month) AS total_new_episodes,
    COUNT(DISTINCT CASE WHEN trends.has_any_new_episode THEN trends.person_id END) AS patients_with_new_episode,
    
    -- Demographics
    AVG(trends.age) AS average_age
)

COMMENT = 'OLIDS Population Trends Semantic View - 60-month time series for condition prevalence, incidence, and multimorbidity trends. Use for over time questions. Grain: person-month.'
