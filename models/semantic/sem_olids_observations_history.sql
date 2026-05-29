{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Clinical Observations History Semantic View
    =================================================

    Serial / over-time companion to sem_olids_observations. Where that view
    holds the LATEST value per biomarker (one row per person), this view holds
    EVERY recorded reading (one row per observation event) so you can answer
    "latest 2", trajectory, variability, and recheck-interval questions.

    OLIDS is the One London Integrated Data Set — primary care data from system
    suppliers (currently EMIS Web, with TPP to follow), unified by the One
    London team.

    Grain: One row per observation event (person x biomarker x date).

    Retention asymmetry (important):
    OLIDS keeps FULL history for currently-registered persons, but only ~5
    years (60 months) for persons who have left or died. Unlike
    sem_olids_appointments and sem_olids_trends, this view is deliberately NOT
    capped at 60 months — that is the point of it, so long-run trajectories for
    current registrants (e.g. a 55-year-old's BMI over 10 years) are usable.
    Consequence: readings older than ~5 years exist mainly for people still
    registered now, so long-run POPULATION cross-sections (e.g. average BMI by
    calendar year over a decade) are survivor-biased. Use this view for
    per-person trajectories and change within the currently-registered cohort
    (filter is_active = TRUE); do not read pre-5-year population averages as
    representative of the population at that time.

    Design:
    - Long format: filter to ONE observation_type before reading `value`.
      Values are only comparable within a single biomarker.
    - For "latest N", window with ROW_NUMBER() OVER (PARTITION BY person_id,
      observation_type ORDER BY clinical_effective_date DESC).
    - Blood pressure is split into 'Systolic BP' and 'Diastolic BP' rows.

    Biomarkers: Systolic BP, Diastolic BP, Total Cholesterol, LDL Cholesterol,
    QRISK, HbA1c, BMI, Waist Circumference, eGFR, Creatinine, Urine ACR, ALT,
    GGT, Bilirubin, Haemoglobin, Platelets, Eosinophils.
#}

TABLES(
    obs AS {{ ref('int_observation_events_long') }}
        PRIMARY KEY (observation_event_id)
        COMMENT = 'One row per biomarker observation event. Filter to a single observation_type before aggregating value.',

    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics, geography, ethnicity, deprivation (current snapshot)'
)

RELATIONSHIPS(
    obs (person_id) REFERENCES demographics
)

FACTS(
    obs.value AS value COMMENT = 'Numeric result. Only comparable within a single observation_type (see unit).'
)

DIMENSIONS(
    -- Observation
    obs.observation_type AS observation_type WITH SYNONYMS = ('biomarker', 'measurement', 'test') COMMENT = 'Biomarker label. Filter to one value before aggregating: Systolic BP, Diastolic BP, Total Cholesterol, LDL Cholesterol, QRISK, HbA1c, BMI, Waist Circumference, eGFR, Creatinine, Urine ACR, ALT, GGT, Bilirubin, Haemoglobin, Platelets, Eosinophils.',
    obs.observation_group AS observation_group COMMENT = 'Clinical group (Cardiovascular, Metabolic, Renal, Liver, Haematology)',
    obs.clinical_effective_date AS clinical_effective_date WITH SYNONYMS = ('observation date', 'test date', 'date') COMMENT = 'Date of the reading',
    obs.unit AS unit COMMENT = 'Unit of measure for value',
    obs.category AS category COMMENT = 'Type-specific clinical category for this reading (meaning varies by observation_type)',

    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age AS age COMMENT = 'Current age in years (drifts — reading dates are event-time)',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands (0-4, 5-9, ..., 80-84, 85+, Unknown)',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands (0-9, 10-19, ..., 70-79, 80+, Unknown)',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS Digital standard age bands (0-4, 5-14, 15-24, ..., 75-84, 85+)',
    demographics.age_band_esp AS age_band_esp COMMENT = 'ESP 2013 age bands (<1, 1-4, 5-9, ..., 80-84, 85-89, 90-94, 95+)',
    demographics.age_life_stage AS age_life_stage COMMENT = 'Life stage (Infant, Toddler, Child, Adolescent, Young Adult, Adult, Older Adult, Elderly, Very Elderly, Unknown)',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (detailed groupings; Unknown/Not Stated/Not Recorded/Refused where missing)',
    demographics.ethnicity_granular AS ethnicity_granular COMMENT = 'Detailed ethnicity classification (Unknown if not recorded)',
    demographics.main_language AS main_language COMMENT = 'Main spoken language (Not Recorded if unknown)',
    demographics.interpreter_needed AS interpreter_needed COMMENT = 'Whether interpreter is required',
    demographics.is_active AS is_active COMMENT = 'Currently registered with NCL GP practice',
    demographics.is_deceased AS is_deceased COMMENT = 'Deceased status',

    -- Organisation
    demographics.practice_code AS practice_code COMMENT = 'GP practice ODS code',
    demographics.practice_name AS practice_name COMMENT = 'GP practice name',
    demographics.pcn_code AS pcn_code COMMENT = 'Primary Care Network code',
    demographics.pcn_name AS pcn_name COMMENT = 'Primary Care Network name',
    demographics.pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'PCN name with borough prefix',
    demographics.borough_registered AS borough_registered COMMENT = 'Registration borough',
    demographics.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB / place-based partnership ODS code of the registered practice: QMJ = NHS North Central London; QRV = NHS North West London. NULL outside the WNL footprint.',
    demographics.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB display name (NHS North Central London or NHS North West London). NULL outside the WNL footprint.',
    demographics.neighbourhood_registered AS neighbourhood_registered COMMENT = 'Registration neighbourhood',

    -- Geography (residence)
    demographics.lsoa_code_21 AS lsoa_code_21 COMMENT = 'Lower Super Output Area 2021 code',
    demographics.ward_code AS ward_code COMMENT = 'Electoral ward 2025 code',
    demographics.ward_name AS ward_name COMMENT = 'Electoral ward 2025 name',
    demographics.borough_resident AS borough_resident COMMENT = 'Residence borough',
    demographics.is_london_resident AS is_london_resident COMMENT = 'Resides in Greater London',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Residence neighbourhood',

    -- Deprivation
    demographics.imd_decile_19 AS imd_decile_19 COMMENT = 'IMD 2019 decile (1=most deprived, 10=least). NULL if LSOA not mapped.',
    demographics.imd_quintile_19 AS imd_quintile_19 COMMENT = 'IMD 2019 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least). Preferred over 2019.',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)'
)

METRICS(
    obs.observation_count AS COUNT(obs.observation_event_id) COMMENT = 'Number of observation events (filter to one observation_type)',
    obs.patient_count AS COUNT(DISTINCT obs.person_id) COMMENT = 'Distinct patients with a reading (filter to one observation_type)',
    obs.avg_value AS AVG(obs.value) COMMENT = 'Average value — only meaningful when filtered to a single observation_type',
    obs.min_value AS MIN(obs.value) COMMENT = 'Minimum value (filter to one observation_type)',
    obs.max_value AS MAX(obs.value) COMMENT = 'Maximum value (filter to one observation_type)'
)

COMMENT = 'OLIDS Clinical Observations History Semantic View - every recorded biomarker reading (one row per person x biomarker x date) for serial, latest-N, trajectory, variability, and recheck-interval analysis. Long format: filter to a single observation_type before reading value. Biomarkers: BP (systolic/diastolic), cholesterol, LDL, QRISK, HbA1c, BMI, waist, eGFR, creatinine, urine ACR, ALT, GGT, bilirubin, haemoglobin, platelets, eosinophils.'
AI_SQL_GENERATION 'Always filter to a single observation_type before aggregating or comparing value — values across types are not comparable (different units). For latest-N questions ("last 2 HbA1c"), use ROW_NUMBER() OVER (PARTITION BY person_id, observation_type ORDER BY clinical_effective_date DESC) and filter the rank. For CHANGE / trajectory questions ("whose BMI or waist circumference rose in the last X months"), per person take the latest reading and a baseline reading (the most recent reading on or before DATEADD(month, -X, latest_date)), then difference them; require both to exist. Blood pressure is two types: Systolic BP and Diastolic BP. Always filter to is_active = TRUE unless asked otherwise. RETENTION: full history is retained for currently-registered persons but only ~5 years for left/deceased persons, so this view is NOT capped at 60 months — use it for per-person trajectories and change within the currently-registered cohort. Do NOT compute long-run population cross-sections (e.g. average value by calendar year going back >5 years) as if representative — pre-5-year data is survivor-biased toward people still registered. This view is for over-time analysis; for the single latest value per biomarker use sem_olids_observations.'
AI_QUESTION_CATEGORIZATION 'Use this view for: serial biomarker readings, latest-2 / last-N values, trends in an individual or cohort over time, variability, rate of change, time between readings, and recheck intervals. For the single most recent value per biomarker (current state) use sem_olids_observations. For condition prevalence/demographics use sem_olids_population. For condition incidence/prevalence trends use sem_olids_trends.'
