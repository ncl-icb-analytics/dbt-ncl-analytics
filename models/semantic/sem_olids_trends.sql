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
    - Prevalence trends over time
    - Incidence trends (new diagnoses per month)
    - Multimorbidity trends
    - Financial year reporting
#}

TABLES(
    trends AS {{ ref('person_month_analysis_base') }}
        PRIMARY KEY (person_id, analysis_month)
        COMMENT = 'Person-month analysis base with 60-month rolling window and condition flags'
)

FACTS(
    trends.total_active_conditions AS total_active_conditions COMMENT = 'Total conditions this month',
    trends.total_new_episodes_this_month AS total_new_episodes COMMENT = 'New condition episodes this month',
    trends.age AS age COMMENT = 'Age at this analysis month'
)

DIMENSIONS(
    -- Time Dimensions
    trends.analysis_month AS analysis_month WITH SYNONYMS = ('month', 'date', 'period') COMMENT = 'Month end date for analysis',
    trends.year_number AS year_number COMMENT = 'Calendar year (e.g., 2024)',
    trends.month_number AS month_number COMMENT = 'Calendar month (1-12)',
    trends.quarter_number AS quarter_number COMMENT = 'Calendar quarter (1-4)',
    trends.month_year_label AS month_year_label COMMENT = 'Readable month-year (e.g., MAR 2024)',
    trends.financial_year AS financial_year WITH SYNONYMS = ('FY', 'fiscal year') COMMENT = 'UK financial year (e.g., 2023/24)',
    trends.financial_year_start AS financial_year_start COMMENT = 'Starting year of financial year',
    trends.financial_quarter AS financial_quarter WITH SYNONYMS = ('FQ', 'fiscal quarter') COMMENT = 'UK financial quarter (Q1=Apr-Jun)',
    trends.financial_quarter_number AS financial_quarter_number COMMENT = 'Financial quarter number (1-4)',
    
    -- Demographics
    trends.gender AS gender COMMENT = 'Patient gender',
    trends.age_band_5y AS age_band_5y COMMENT = '5-year age bands',
    trends.age_band_10y AS age_band_10y COMMENT = '10-year age bands',
    trends.age_band_nhs AS age_band_nhs COMMENT = 'NHS standard age bands',
    trends.age_life_stage AS age_life_stage COMMENT = 'Life stage classification',
    
    -- Ethnicity
    trends.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category',
    trends.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory',
    
    -- Organisation
    trends.practice_code AS practice_code COMMENT = 'GP practice code',
    trends.practice_name AS practice_name COMMENT = 'GP practice name',
    trends.pcn_code AS pcn_code COMMENT = 'PCN code',
    trends.pcn_name AS pcn_name COMMENT = 'PCN name',
    trends.pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'PCN with borough prefix',
    trends.borough_registered AS borough_registered COMMENT = 'Borough of registration',
    trends.neighbourhood_registered AS neighbourhood_registered COMMENT = 'NCL neighbourhood',
    
    -- Geography
    trends.lsoa_code_21 AS lsoa_code_21 COMMENT = 'LSOA 2021 code',
    trends.ward_code AS ward_code COMMENT = 'Electoral ward code',
    trends.ward_name AS ward_name COMMENT = 'Electoral ward name',
    trends.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Residence neighbourhood',
    
    -- Deprivation
    trends.imd_decile_19 AS imd_decile_19 COMMENT = 'IMD 2019 decile',
    trends.imd_quintile_19 AS imd_quintile_19 COMMENT = 'IMD 2019 quintile',
    trends.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile',
    trends.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile',
    
    -- Registration Status
    trends.is_active AS is_active COMMENT = 'Active registration this month',
    trends.is_deceased AS is_deceased COMMENT = 'Deceased by this month',
    
    -- Condition Prevalence Flags
    trends.has_ast AS has_asthma COMMENT = 'Has asthma this month',
    trends.has_copd AS has_copd WITH SYNONYMS = ('COPD') COMMENT = 'Has COPD this month',
    trends.has_htn AS has_hypertension WITH SYNONYMS = ('HTN', 'high blood pressure') COMMENT = 'Has hypertension this month',
    trends.has_chd AS has_chd WITH SYNONYMS = ('CHD', 'coronary heart disease') COMMENT = 'Has CHD this month',
    trends.has_af AS has_af WITH SYNONYMS = ('AF', 'atrial fibrillation') COMMENT = 'Has AF this month',
    trends.has_hf AS has_heart_failure COMMENT = 'Has heart failure this month',
    trends.has_pad AS has_pad COMMENT = 'Has PAD this month',
    trends.has_dm AS has_diabetes WITH SYNONYMS = ('DM', 'diabetic') COMMENT = 'Has diabetes this month',
    trends.has_ndh AS has_ndh WITH SYNONYMS = ('prediabetes') COMMENT = 'Has NDH this month',
    trends.has_dep AS has_depression COMMENT = 'Has depression this month',
    trends.has_smi AS has_smi WITH SYNONYMS = ('SMI', 'severe mental illness') COMMENT = 'Has SMI this month',
    trends.has_ckd AS has_ckd WITH SYNONYMS = ('CKD', 'chronic kidney disease') COMMENT = 'Has CKD this month',
    trends.has_dem AS has_dementia COMMENT = 'Has dementia this month',
    trends.has_ep AS has_epilepsy COMMENT = 'Has epilepsy this month',
    trends.has_stia AS has_stroke_tia COMMENT = 'Has stroke/TIA this month',
    trends.has_can AS has_cancer COMMENT = 'Has cancer this month',
    trends.has_pc AS has_palliative_care COMMENT = 'On palliative care this month',
    trends.has_ld AS has_learning_disability WITH SYNONYMS = ('LD') COMMENT = 'Has LD this month',
    trends.has_frail AS has_frailty COMMENT = 'Has frailty this month',
    trends.has_ra AS has_ra COMMENT = 'Has RA this month',
    trends.has_ost AS has_osteoporosis COMMENT = 'Has osteoporosis this month',
    trends.has_nafld AS has_nafld COMMENT = 'Has NAFLD this month',
    trends.has_fh AS has_fh COMMENT = 'Has familial hypercholesterolaemia this month',
    
    -- Incidence Flags
    trends.new_ast AS new_asthma COMMENT = 'New asthma this month',
    trends.new_copd AS new_copd COMMENT = 'New COPD this month',
    trends.new_htn AS new_hypertension COMMENT = 'New hypertension this month',
    trends.new_chd AS new_chd COMMENT = 'New CHD this month',
    trends.new_af AS new_af COMMENT = 'New AF this month',
    trends.new_hf AS new_heart_failure COMMENT = 'New heart failure this month',
    trends.new_pad AS new_pad COMMENT = 'New PAD this month',
    trends.new_dm AS new_diabetes COMMENT = 'New diabetes this month',
    trends.new_ndh AS new_ndh COMMENT = 'New NDH this month',
    trends.new_gestdiab AS new_gestational_diabetes COMMENT = 'New gestational diabetes this month',
    trends.new_dep AS new_depression COMMENT = 'New depression this month',
    trends.new_smi AS new_smi COMMENT = 'New SMI this month',
    trends.new_ckd AS new_ckd COMMENT = 'New CKD this month',
    trends.new_dem AS new_dementia COMMENT = 'New dementia this month',
    trends.new_ep AS new_epilepsy COMMENT = 'New epilepsy this month',
    trends.new_stia AS new_stroke_tia COMMENT = 'New stroke/TIA this month',
    trends.new_can AS new_cancer COMMENT = 'New cancer this month',
    trends.new_pc AS new_palliative_care COMMENT = 'New palliative care this month',
    trends.new_ld AS new_learning_disability COMMENT = 'New LD this month',
    trends.new_frail AS new_frailty COMMENT = 'New frailty this month',
    trends.new_ra AS new_ra COMMENT = 'New RA this month',
    trends.new_ost AS new_osteoporosis COMMENT = 'New osteoporosis this month',
    trends.new_nafld AS new_nafld COMMENT = 'New NAFLD this month',
    trends.new_fh AS new_fh COMMENT = 'New FH this month',
    
    -- Summary Flags
    trends.has_any_condition AS has_any_condition COMMENT = 'Has any condition this month',
    trends.has_any_new_episode AS has_any_new_episode COMMENT = 'Has any new episode this month'
)

METRICS(
    -- Population
    patient_count AS COUNT(DISTINCT trends.person_id) COMMENT = 'Total patients in period',
    
    -- Prevalence
    diabetes_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_dm THEN trends.person_id END) COMMENT = 'Patients with diabetes',
    hypertension_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_htn THEN trends.person_id END) COMMENT = 'Patients with hypertension',
    copd_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_copd THEN trends.person_id END) COMMENT = 'Patients with COPD',
    asthma_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ast THEN trends.person_id END) COMMENT = 'Patients with asthma',
    chd_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_chd THEN trends.person_id END) COMMENT = 'Patients with CHD',
    af_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_af THEN trends.person_id END) COMMENT = 'Patients with AF',
    heart_failure_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_hf THEN trends.person_id END) COMMENT = 'Patients with heart failure',
    pad_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_pad THEN trends.person_id END) COMMENT = 'Patients with PAD',
    ckd_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ckd THEN trends.person_id END) COMMENT = 'Patients with CKD',
    ndh_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ndh THEN trends.person_id END) COMMENT = 'Patients with NDH',
    depression_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_dep THEN trends.person_id END) COMMENT = 'Patients with depression',
    smi_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_smi THEN trends.person_id END) COMMENT = 'Patients with SMI',
    dementia_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_dem THEN trends.person_id END) COMMENT = 'Patients with dementia',
    cancer_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_can THEN trends.person_id END) COMMENT = 'Patients with cancer',
    frailty_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_frail THEN trends.person_id END) COMMENT = 'Patients with frailty',
    learning_disability_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ld THEN trends.person_id END) COMMENT = 'Patients with LD',
    epilepsy_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ep THEN trends.person_id END) COMMENT = 'Patients with epilepsy',
    stroke_tia_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_stia THEN trends.person_id END) COMMENT = 'Patients with stroke/TIA',
    palliative_care_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_pc THEN trends.person_id END) COMMENT = 'Patients on palliative care',
    ra_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ra THEN trends.person_id END) COMMENT = 'Patients with RA',
    osteoporosis_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ost THEN trends.person_id END) COMMENT = 'Patients with osteoporosis',
    nafld_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_nafld THEN trends.person_id END) COMMENT = 'Patients with NAFLD',
    fh_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_fh THEN trends.person_id END) COMMENT = 'Patients with FH',
    
    -- Incidence
    diabetes_incidence AS COUNT(DISTINCT CASE WHEN trends.new_dm THEN trends.person_id END) COMMENT = 'New diabetes diagnoses',
    hypertension_incidence AS COUNT(DISTINCT CASE WHEN trends.new_htn THEN trends.person_id END) COMMENT = 'New hypertension diagnoses',
    copd_incidence AS COUNT(DISTINCT CASE WHEN trends.new_copd THEN trends.person_id END) COMMENT = 'New COPD diagnoses',
    asthma_incidence AS COUNT(DISTINCT CASE WHEN trends.new_ast THEN trends.person_id END) COMMENT = 'New asthma diagnoses',
    chd_incidence AS COUNT(DISTINCT CASE WHEN trends.new_chd THEN trends.person_id END) COMMENT = 'New CHD diagnoses',
    af_incidence AS COUNT(DISTINCT CASE WHEN trends.new_af THEN trends.person_id END) COMMENT = 'New AF diagnoses',
    heart_failure_incidence AS COUNT(DISTINCT CASE WHEN trends.new_hf THEN trends.person_id END) COMMENT = 'New heart failure diagnoses',
    pad_incidence AS COUNT(DISTINCT CASE WHEN trends.new_pad THEN trends.person_id END) COMMENT = 'New PAD diagnoses',
    ckd_incidence AS COUNT(DISTINCT CASE WHEN trends.new_ckd THEN trends.person_id END) COMMENT = 'New CKD diagnoses',
    depression_incidence AS COUNT(DISTINCT CASE WHEN trends.new_dep THEN trends.person_id END) COMMENT = 'New depression diagnoses',
    smi_incidence AS COUNT(DISTINCT CASE WHEN trends.new_smi THEN trends.person_id END) COMMENT = 'New SMI diagnoses',
    dementia_incidence AS COUNT(DISTINCT CASE WHEN trends.new_dem THEN trends.person_id END) COMMENT = 'New dementia diagnoses',
    epilepsy_incidence AS COUNT(DISTINCT CASE WHEN trends.new_ep THEN trends.person_id END) COMMENT = 'New epilepsy diagnoses',
    cancer_incidence AS COUNT(DISTINCT CASE WHEN trends.new_can THEN trends.person_id END) COMMENT = 'New cancer diagnoses',
    stroke_tia_incidence AS COUNT(DISTINCT CASE WHEN trends.new_stia THEN trends.person_id END) COMMENT = 'New stroke/TIA',
    learning_disability_incidence AS COUNT(DISTINCT CASE WHEN trends.new_ld THEN trends.person_id END) COMMENT = 'New LD diagnoses',
    frailty_incidence AS COUNT(DISTINCT CASE WHEN trends.new_frail THEN trends.person_id END) COMMENT = 'New frailty diagnoses',
    palliative_care_incidence AS COUNT(DISTINCT CASE WHEN trends.new_pc THEN trends.person_id END) COMMENT = 'New palliative care',
    
    -- Multimorbidity
    avg_conditions_per_patient AS AVG(trends.total_active_conditions) COMMENT = 'Average conditions per patient',
    multimorbidity_count AS COUNT(DISTINCT CASE WHEN trends.total_active_conditions >= 2 THEN trends.person_id END) COMMENT = 'Patients with 2+ conditions',
    complex_multimorbidity_count AS COUNT(DISTINCT CASE WHEN trends.total_active_conditions >= 4 THEN trends.person_id END) COMMENT = 'Patients with 4+ conditions',
    
    -- Total Incidence
    total_new_episodes AS SUM(trends.total_new_episodes_this_month) COMMENT = 'Total new episodes',
    patients_with_new_episode AS COUNT(DISTINCT CASE WHEN trends.has_any_new_episode THEN trends.person_id END) COMMENT = 'Patients with any new episode',
    
    -- Demographics
    average_age AS AVG(trends.age) COMMENT = 'Average age'
)

COMMENT = 'OLIDS Population Trends Semantic View - 60-month time series for condition prevalence, incidence, and multimorbidity trends. Grain: one row per person per month.'
AI_SQL_GENERATION 'Use financial_year and financial_quarter for UK reporting periods. For trend queries, group by analysis_month or financial_year. Prevalence metrics count patients WITH a condition; incidence metrics count NEW diagnoses. Always filter to is_active = TRUE unless analysing deceased patients.'
AI_QUESTION_CATEGORIZATION 'Use this view for questions about: trends over time, prevalence changes, incidence rates, year-on-year comparisons, financial year reporting, and monthly/quarterly analysis. For current state snapshots use sem_olids_population. For clinical biomarkers use sem_olids_observations.'
