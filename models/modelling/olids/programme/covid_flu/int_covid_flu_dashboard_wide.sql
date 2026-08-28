{{
    config(
        materialized='table',
        tags=['covid_flu'],
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
--TESTING NEW DASHBOARD MODEL with WIDE RISK GROUPS - NEED TO SPLIT FURTHER INTO SUBCOHORTS AND RISK GROUPS.

----1,027,933 historical people with a vaccination record (2026/27 not yet added)

with subcohort as (
select person_id,
--Sub Cohort Clinical groups may change over time as new campaigns are added.
    MAX(IFF(subcohort in ( 'Active Asthma Management', 'Asthma','Asthma Admission' ), 1, 0)) AS has_asthma,
    MAX(IFF(subcohort in ('Asplenia/Spleen Dysfunction','Asplenia'), 1, 0)) AS has_asplenia,
    MAX(IFF(subcohort = 'Chronic Heart Disease', 1, 0)) AS has_chd,
    MAX(IFF(subcohort in ('Chronic Kidney Disease', 'Chronic Kidney Disease (Stage 3-5)'), 1, 0)) AS has_ckd,
    MAX(IFF(subcohort = 'Chronic Liver Disease', 1, 0)) AS has_cld,
    MAX(IFF(subcohort = 'Chronic Neurological Disease', 1, 0)) AS has_cnd,
    MAX(IFF(subcohort = 'Chronic Respiratory Disease', 1, 0)) AS has_crd,
    MAX(IFF(subcohort in ('Diabetes','Gestational Diabetes'), 1, 0)) AS has_diabetes,
    MAX(IFF(subcohort = 'Immunosuppression', 1, 0)) AS is_immunosuppressed,
    MAX(IFF(subcohort = 'Learning Disability', 1, 0)) AS has_ld
FROM {{ ref('fct_covid_flu_uptake') }} 
--FROM REPORTING.OLIDS_PROGRAMME.FCT_COVID_FLU_UPTAKE
GROUP BY all
 order by 1
)
,VACC_POP as (
SELECT 
    s.person_id,
    -- Eligibility information from uptake facts
    cf.is_eligible,
    CASE WHEN cf.campaign_id = 'COVID Autumn 2024' THEN 'CV Autumn 2024'
         WHEN cf.campaign_id = 'COVID Spring 2025' THEN 'CV Spring 2025'
         WHEN cf.campaign_id = 'COVID Autumn 2025' THEN 'CV Autumn 2025'
         WHEN cf.campaign_id = 'COVID Spring 2026' THEN 'CV Spring 2026'
         ELSE cf.campaign_id END AS campaign_id,
    CASE 
        WHEN cf.campaign_id = 'Flu 2024-25' THEN 1
        WHEN cf.campaign_id = 'COVID Autumn 2024' THEN 2
        WHEN cf.campaign_id = 'COVID Spring 2025' THEN 3
        WHEN cf.campaign_id = 'Flu 2025-26' THEN 4
        WHEN cf.campaign_id = 'COVID Autumn 2025' THEN 5
        WHEN cf.campaign_id = 'COVID Spring 2026' THEN 6
        END AS campaign_sort,
    cf.programme_type,
    cf.campaign_year,
    cf.campaign_season,
     -- Vaccination information from uptake facts
    cf.vaccination_status,
    DATE(cf.vaccination_date) AS vaccination_date,
    TO_CHAR(TO_DATE(cf.vaccination_date), 'MMMM') as vaccination_month,
    YEAR(cf.vaccination_date) AS vaccination_year,
    cf.vaccination_status_reason,
    cf.vaccinated_despite_ineligible,
    -- Uptake flags and metrics from uptake facts
    cf.vaccinated,
    cf.declined,
    cf.eligible_no_record,
    cf.uptake_category,
    cf.days_to_vaccination,
    -- Programme-specific fields
    -- cf.laiv_given, 
    -- Campaign dates
    cf.campaign_start_date,
    cf.campaign_reference_date,
   cf.risk_group,
   --add subcohort clinical flags for dashboard filtering and analysis.
   s.has_asthma,
   s.has_asplenia,
   s.has_chd,
   s.has_ckd,
   s.has_cld,
   s.has_cnd,
   s.has_crd,
   s.has_diabetes,
   s.is_immunosuppressed
   
FROM Subcohort s
--LEFT JOIN REPORTING.OLIDS_PROGRAMME.FCT_COVID_FLU_UPTAKE cf using (person_id)
LEFT JOIN {{ ref('fct_covid_flu_uptake') }} cf using (person_id)
where risk_group   not in ('Active Asthma Management', 'Asthma','Asthma Admission', 
      'Asplenia/Spleen Dysfunction','Asplenia',
      'Chronic Heart Disease', 
      'Chronic Kidney Disease', 'Chronic Kidney Disease (Stage 3-5)',
      'Chronic Liver Disease', 
      'Chronic Neurological Disease' ,
      'Chronic Respiratory Disease', 
    'Diabetes','Gestational Diabetes',
    'Immunosuppression'
    )  
 group by all
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

