{{
    config(
        materialized='table',
        cluster_by=['programme_type', 'campaign_id', 'practice_code', 'person_id']
    )
}}
/*
COVID and Flu Dashboard Base Table

This model provides dashboard-ready data by combining the pure uptake facts
with demographics and practice information for the COVID and Flu Dashboard.

Key features:
- Joins fct_covid_flu_uptake with dim_person_demographics
- Provides complete demographic, geographic and practice dimensions
- Optimised clustering for dashboard query patterns
- Single source of truth for COVID/Flu dashboard consumption

Multi-Programme Support:
COVID Campaigns:
- COVID Autumn 2024, COVID Spring 2025, COVID Autumn 2025, COVID Spring 2026

Flu Campaigns: 
- Flu 2023-24, Flu 2024-25, Flu 2025-26

Usage:
- Primary table for COVID and Flu Dashboard in PowerBI/Tableau
- Filter by programme_type for programme-specific dashboards
- Use campaign_id for specific campaign analysis
- Demographic breakdowns for equity analysis

PowerBI
Testing WIDE FORMAT for RISK GROUP using columns and flags. 
*/
--TESTING NEW DASHBOARD MODEL with WIDE RISK GROUPS

----1,027,933 historical people with a vaccination record (2026/27 not yet added)
with vacc_pop as (
SELECT
    person_id,
    -- Eligibility information from uptake facts
    is_eligible,
    CASE WHEN campaign_id = 'COVID Autumn 2024' THEN 'CV Autumn 2024'
         WHEN campaign_id = 'COVID Spring 2025' THEN 'CV Spring 2025'
         WHEN campaign_id = 'COVID Autumn 2025' THEN 'CV Autumn 2025'
         WHEN campaign_id = 'COVID Spring 2026' THEN 'CV Spring 2026'
         ELSE campaign_id END AS campaign_id,
    CASE 
        WHEN u.campaign_id = 'Flu 2024-25' THEN 1
        WHEN u.campaign_id = 'COVID Autumn 2024' THEN 2
        WHEN u.campaign_id = 'COVID Spring 2025' THEN 3
        WHEN u.campaign_id = 'Flu 2025-26' THEN 4
        WHEN u.campaign_id = 'COVID Autumn 2025' THEN 5
        WHEN u.campaign_id = 'COVID Spring 2026' THEN 6
        END AS campaign_sort,
    programme_type,
    campaign_year,
    campaign_season,
     -- Vaccination information from uptake facts
    vaccination_status,
    DATE(vaccination_date) AS vaccination_date,
    TO_CHAR(TO_DATE(vaccination_date), 'MMMM') as vaccination_month,
    YEAR(vaccination_date) AS vaccination_year,
    vaccination_status_reason,
    vaccinated_despite_ineligible,
    -- Uptake flags and metrics from uptake facts
    vaccinated,
    declined,
    eligible_no_record,
    uptake_category,
    days_to_vaccination,
    -- Programme-specific fields
    laiv_given, 
    -- Campaign dates
    campaign_start_date,
    campaign_reference_date,
    --Risk groups may change over time as new campaigns are added.
    MAX(IFF(risk_group = 'Active Asthma Management', 1, 0)) AS active_asthma_management_flag,
    MAX(IFF(risk_group = 'Age 65 and Over', 1, 0)) AS age_65_and_over_flag,
    MAX(IFF(risk_group = 'Age 75+', 1, 0)) AS age_75_plus_flag,
    MAX(IFF(risk_group = 'Asplenia', 1, 0)) AS asplenia_flag,
    MAX(IFF(risk_group = 'Asplenia/Spleen Dysfunction', 1, 0)) AS asplenia_spleen_dysfunction_flag,
    MAX(IFF(risk_group = 'Asthma', 1, 0)) AS asthma_flag,
    MAX(IFF(risk_group = 'Asthma Admission', 1, 0)) AS asthma_admission_flag,
    MAX(IFF(risk_group = 'Carer', 1, 0)) AS carer_flag,
    MAX(IFF(risk_group = 'Children Preschool', 1, 0)) AS children_preschool_flag,
    MAX(IFF(risk_group = 'Children School Age', 1, 0)) AS children_school_age_flag,
    MAX(IFF(risk_group = 'Chronic Heart Disease', 1, 0)) AS chronic_heart_disease_flag,
    MAX(IFF(risk_group = 'Chronic Kidney Disease', 1, 0)) AS chronic_kidney_disease_flag,
    MAX(IFF(risk_group = 'Chronic Kidney Disease (Stage 3-5)', 1, 0)) AS chronic_kidney_disease_stage_3_5_flag,
    MAX(IFF(risk_group = 'Chronic Liver Disease', 1, 0)) AS chronic_liver_disease_flag,
    MAX(IFF(risk_group = 'Chronic Neurological Disease', 1, 0)) AS chronic_neurological_disease_flag,
    MAX(IFF(risk_group = 'Chronic Respiratory Disease', 1, 0)) AS chronic_respiratory_disease_flag,
    MAX(IFF(risk_group = 'Diabetes', 1, 0)) AS diabetes_flag,
    MAX(IFF(risk_group = 'Gestational Diabetes', 1, 0)) AS gestational_diabetes_flag,
    MAX(IFF(risk_group = 'Health and Social Care Workers', 1, 0)) AS health_social_care_workers_flag,
    MAX(IFF(risk_group in ('Homeless', 'Homelessness'), 1, 0)) AS homeless_flag,
    MAX(IFF(risk_group = 'Household Contact Immunocompromised', 1, 0)) AS household_contact_immunocompromised_flag,
    MAX(IFF(risk_group = 'Immunosuppression', 1, 0)) AS immunosuppression_flag,
    MAX(IFF(risk_group = 'Learning Disability', 1, 0)) AS learning_disability_flag,
    MAX(IFF(risk_group in ('Long Term Residential Care', 'Long-term Residential Care'), 1, 0)) AS long_term_residential_care_flag,
    MAX(IFF(risk_group = 'Morbid Obesity', 1, 0)) AS morbid_obesity_flag,
    MAX(IFF(risk_group = 'Pregnancy', 1, 0)) AS pregnancy_flag,
    MAX(IFF(risk_group = 'Severe Mental Illness', 1, 0)) AS severe_mental_illness_flag,
    MAX(IFF(risk_group = 'Under 65 At Risk', 1, 0)) AS under_65_at_risk_flag,
    MAX(IFF(risk_group = 'Vaccinated Despite Ineligibility', 1, 0)) AS vaccinated_despite_ineligibility_flag
FROM {{ ref('fct_covid_flu_uptake') }} 
--FROM REPORTING.OLIDS_PROGRAMME.FCT_COVID_FLU_UPTAKE
GROUP BY all
 order by 1
)
--add in demographics.
SELECT 
        v.*,
        id.hx_flake,
        -- Demographics from dim_person_demographics
        d.is_active,
        d.gender,
        d.age,
        d.age_band_5y,
        d.age_band_10y,
        d.age_band_nhs,
        d.ethnicity_category,
        CASE 
        WHEN d.ETHNICITY_CATEGORY = 'Asian' THEN 1
        WHEN d.ETHNICITY_CATEGORY = 'Black' THEN 2
        WHEN d.ETHNICITY_CATEGORY = 'Mixed' THEN 3
        WHEN d.ETHNICITY_CATEGORY = 'Other' THEN 4
        WHEN d.ETHNICITY_CATEGORY = 'White' THEN 5
        WHEN d.ETHNICITY_CATEGORY = 'Unknown' THEN 6
        END AS ethcat_order, 
        CASE
        WHEN d.ETHNICITY_SUBCATEGORY in ('Not Recorded','Not stated','Not Stated','Recorded Not Known','Refused') THEN 'Unknown'
        ELSE d.ETHNICITY_SUBCATEGORY END AS ethnicity_subcategory,
        CASE
        WHEN d.ethnicity_granular = 'MENA' THEN 'Middle East and North African'
        WHEN d.ethnicity_granular in ('Muslim', 'Sikh') THEN 'Unknown'
        WHEN d.ethnicity_granular in ('Recorded Not Known', 'Refused', 'Not stated', 'Not Recorded','Not Stated') THEN 'Unknown'
        WHEN d.ethnicity_granular = 'Black - E.African Asian' THEN 'East African Asian'
        WHEN d.ethnicity_granular = 'Indo-Caribbean' THEN 'Caribbean Asian'
        WHEN d.ethnicity_granular = 'Cornish' THEN 'English'
        WHEN d.ethnicity_granular in ('Gypsy or Irish Traveller','Gypsy','Irish Traveller') THEN 'Gypsy or Irish Traveller'
        WHEN d.ethnicity_granular in ('Albanian/Serbian', 'Serbian') THEN 'Albanian or Serbian'
        ELSE d.ethnicity_granular END AS ethnicity_granular,
        CASE 
        WHEN d.main_language = 'Pushto' THEN 'Pashto' 
        WHEN d.main_language = 'Gujerati' THEN 'Gujarati'
        WHEN d.main_language ILIKE '%sign language%' THEN 'Sign language'
        WHEN d.main_language = 'Norwegian Bokmål' THEN 'Norwegian'
        WHEN d.main_language = 'Not Recorded' THEN 'Unknown'
        ELSE d.main_language END AS main_language,
        d.language_type,
        d.interpreter_needed,
        d.interpreter_type,
        
        -- Geographic and deprivation (residence-based)
        d.imd_quintile_25,
        d.imd_decile_25,
        CASE 
        WHEN d.IMD_QUINTILE_25 = 'Most Deprived' THEN 1
        WHEN d.IMD_QUINTILE_25 = 'Second Most Deprived' THEN 2
        WHEN d.IMD_QUINTILE_25 = 'Third Most Deprived' THEN 3
        WHEN d.IMD_QUINTILE_25 = 'Second Least Deprived' THEN 4
        WHEN d.IMD_QUINTILE_25 = 'Least Deprived' THEN 5
        ELSE 6 END AS imdquintile_order,
        d.lsoa_code_21 as lsoa_code,
        d.lsoa_name_21 as lsoa_name,
        d.ward_code,
        d.ward_name,
        COALESCE(la.LAD25_NM,'Unknown') AS borough_resident,
        CASE WHEN la.RESIDENT_FLAG IS NULL THEN 'Unknown'
        ELSE la.RESIDENT_FLAG END as residential_loc,
        d.neighbourhood_resident,
        -- d.icb_code_resident,
        -- d.icb_resident,

        -- Practice information (registration-based)
        d.practice_code,
        d.practice_name,
        d.pcn_code,
        d.pcn_name,
        d.borough_registered,
        d.neighbourhood_registered,
        
        -- School information from dim_person_age (Early Years for ages 2-3, Reception+ for school ages)
        pa.age_school_stage AS school_year,
        pa.is_early_years_age,
        pa.is_primary_school_age,
        pa.is_secondary_school_age,
    
        -- Housebound status from dim_person_housebound_status
        COALESCE(hs.is_housebound, FALSE) AS is_housebound
FROM vacc_pop v
--LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS d ON d.person_id = v.person_id
LEFT JOIN {{ ref('dim_person_demographics') }} d ON d.person_id = v.person_id
--LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_PSEUDO id  ON d.person_id = id.person_id
LEFT JOIN {{ ref('person_pseudo') }} id  ON d.person_id = id.person_id      
--LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_AGE pa ON d.person_id = pa.person_id
LEFT JOIN {{ ref('dim_person_age') }} pa ON d.person_id = pa.person_id
--LEFT JOIN REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_HOUSEBOUND_STATUS hs ON d.person_id = hs.person_id
LEFT JOIN {{ ref('dim_person_housebound_status') }} hs ON d.person_id = hs.person_id
--LEFT JOIN STAGING.REFERENCE.STG_REFERENCE_LSOA21_WARD25_LAD25 la on la.LSOA21_CD = d.LSOA_CODE_21
LEFT JOIN {{ ref('stg_reference_lsoa21_ward25_lad25') }} la on la.LSOA21_CD = d.LSOA_CODE_21

