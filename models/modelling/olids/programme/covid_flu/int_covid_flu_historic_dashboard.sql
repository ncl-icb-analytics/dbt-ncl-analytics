{{
    config(
        materialized='table',
        tags=['covid_flu'],
        cluster_by=['programme_type', 'campaign_id', 'practice_code', 'person_id']
    )
}}
/*This is a snapshot of the COVID and FLU Dashboard base for the analysis month end at 2026-09-30. 
Includes left and died so we can compared COVID and FLU vaccination uptake from 2024-25 season with the HEI snapshot from 26.09.2025 
*/
--historical population snapshot for 2025-09-30
with population as (
select person_id,
analysis_month,
age,
age_band_5y,
age_band_10y,
is_early_years_age,
is_primary_school_age,
is_secondary_school_age,
gender,
ethnicity_category,
ethnicity_subcategory,
ethnicity_granular,
main_language,
is_active,
is_deceased,
borough_registered,
neighbourhood_registered,
practice_code,
practice_name,
pcn_name,
lsoa_code_21,
lsoa_name_21,
ward_code,
ward_name,
imd_quintile_25,
imd_decile_25,
-- --extra LTCS
-- has_ckd,
-- has_chd,
-- has_dm,
-- has_ast
--from REPORTING.OLIDS_PERSON_ANALYTICS.PERSON_MONTH_ANALYSIS_BASE
FROM {{ ref('person_month_analysis_base') }}
where analysis_month = '2025-09-30'
)

,uptake_with_demographics AS (
    SELECT 
        --population snaphot for 2025-09-30
        p.person_id,
        p.analysis_month,
        p.is_active,
        p.is_deceased,
        -- Programme and campaign identifiers
        u.programme_type,
        CASE WHEN u.campaign_id = 'COVID Autumn 2024' THEN 'CV Autumn 2024'
         WHEN u.campaign_id = 'COVID Spring 2025' THEN 'CV Spring 2025'
         WHEN u.campaign_id = 'COVID Autumn 2025' THEN 'CV Autumn 2025'
         WHEN u.campaign_id = 'COVID Spring 2026' THEN 'CV Spring 2026'
         ELSE u.campaign_id END AS campaign_id,
        u.campaign_year,
        u.campaign_season,

        -- Demographics from population snapshot
        p.gender,
        p.age,
        p.age_band_5y,
        p.age_band_10y,
        p.ethnicity_category,
        CASE 
        WHEN p.ETHNICITY_CATEGORY = 'Asian' THEN 1
        WHEN p.ETHNICITY_CATEGORY = 'Black' THEN 2
        WHEN p.ETHNICITY_CATEGORY = 'Mixed' THEN 3
        WHEN p.ETHNICITY_CATEGORY = 'Other' THEN 4
        WHEN p.ETHNICITY_CATEGORY = 'White' THEN 5
        WHEN p.ETHNICITY_CATEGORY = 'Unknown' THEN 6
        END AS ethcat_order, 
        CASE
        WHEN p.ETHNICITY_SUBCATEGORY in ('Not Recorded','Not stated','Not Stated','Recorded Not Known','Refused') THEN 'Unknown'
        ELSE p.ETHNICITY_SUBCATEGORY END AS ethnicity_subcategory,
        CASE
        WHEN p.ethnicity_granular = 'MENA' THEN 'Middle East and North African'
        WHEN p.ethnicity_granular in ('Muslim', 'Sikh') THEN 'Unknown'
        WHEN p.ethnicity_granular in ('Recorded Not Known', 'Refused', 'Not stated', 'Not Recorded','Not Stated') THEN 'Unknown'
        WHEN p.ethnicity_granular = 'Black - E.African Asian' THEN 'East African Asian'
        WHEN p.ethnicity_granular = 'Indo-Caribbean' THEN 'Caribbean Asian'
        WHEN p.ethnicity_granular = 'Cornish' THEN 'English'
        WHEN p.ethnicity_granular in ('Gypsy or Irish Traveller','Gypsy','Irish Traveller') THEN 'Gypsy or Irish Traveller'
        WHEN p.ethnicity_granular in ('Albanian/Serbian', 'Serbian') THEN 'Albanian or Serbian'
        ELSE p.ethnicity_granular END AS ethnicity_granular,
        CASE 
        WHEN p.main_language = 'Pushto' THEN 'Pashto' 
        WHEN p.main_language = 'Gujerati' THEN 'Gujarati'
        WHEN p.main_language ILIKE '%sign language%' THEN 'Sign language'
        WHEN p.main_language = 'Norwegian Bokmål' THEN 'Norwegian'
        WHEN p.main_language = 'Not Recorded' THEN 'Unknown'
        ELSE p.main_language END AS main_language,
        
        -- Geographic and deprivation (residence-based)
        p.imd_quintile_25,
        p.imd_decile_25,
        CASE 
        WHEN p.IMD_QUINTILE_25 = 'Most Deprived' THEN 1
        WHEN p.IMD_QUINTILE_25 = 'Second Most Deprived' THEN 2
        WHEN p.IMD_QUINTILE_25 = 'Third Most Deprived' THEN 3
        WHEN p.IMD_QUINTILE_25 = 'Second Least Deprived' THEN 4
        WHEN p.IMD_QUINTILE_25 = 'Least Deprived' THEN 5
        ELSE 6 END AS imdquintile_order,
        p.lsoa_code_21 as lsoa_code,
        p.lsoa_name_21 as lsoa_name,
        p.ward_code,
        p.ward_name,
        COALESCE(la.LAD25_NM,'Unknown') AS borough_resident,
        CASE WHEN la.RESIDENT_FLAG IS NULL THEN 'Unknown'
        ELSE la.RESIDENT_FLAG END as residential_loc,

        -- Practice information (registration-based)
        p.practice_code,
        p.practice_name,
        p.pcn_name,
        p.borough_registered,
        p.neighbourhood_registered,
        
        -- School information from dim_person_age (Early Years for ages 2-3, Reception+ for school ages)
        p.is_early_years_age,
        p.is_primary_school_age,
        p.is_secondary_school_age,
        -- -- extra LTCS
        -- p.has_ckd,
        -- p.has_chd,
        -- p.has_dm,
        -- p.has_ast,
        -- Flu vaccination setting (Early Years at GP for ages 2-3, School-based for Reception-Year 11)
        CASE
            WHEN u.programme_type = 'FLU' THEN
                CASE
                    WHEN p.age >= 2 AND p.age < 4 THEN 'Early Years (GP)'  -- Ages 2-3
                    WHEN p.is_primary_school_age and p.is_secondary_school_age THEN 'School-based'
                    ELSE NULL  -- Year 12, Year 13, and older
                END
            ELSE NULL
        END AS flu_vaccination_setting,

        -- -- Housebound status from dim_person_housebound_status
        -- COALESCE(hs.is_housebound, FALSE) AS is_housebound,
        
        -- Eligibility information from uptake facts
        u.is_eligible,
        u.campaign_category,
        u.risk_group,
        u.subcohort,
        --u.eligibility_reason,
        --u.rule_type,
        
        -- Vaccination information from uptake facts
        u.vaccination_status,
        DATE(u.vaccination_date) AS vaccination_date,
        TO_CHAR(TO_DATE(vaccination_date), 'MMMM') as vaccination_month,
        YEAR(vaccination_date) AS vaccination_year,
        u.vaccination_status_reason,
        u.vaccinated_despite_ineligible,
        
        -- Uptake flags and metrics from uptake facts
        u.vaccinated,
        u.declined,
        u.eligible_no_record,
        u.uptake_category,
        u.days_to_vaccination,
        
        -- -- Programme-specific fields
        -- u.laiv_given,
        
        -- Campaign dates
        u.campaign_start_date,
        u.campaign_reference_date
        -- u.audit_end_date,
        -- u.created_at
        
    FROM {{ ref('fct_covid_flu_uptake') }} u
    INNER JOIN population p ON u.person_id = p.person_id
    -- LEFT JOIN {{ ref('dim_person_demographics') }} d ON p.person_id = d.person_id
    -- LEFT JOIN {{ ref('person_pseudo') }} id  ON u.person_id = id.person_id
    -- LEFT JOIN {{ ref('dim_person_age') }} pa ON p.person_id = pa.person_id
    -- LEFT JOIN {{ ref('dim_person_housebound_status') }} hs ON p.person_id = hs.person_id
    LEFT JOIN {{ ref('stg_reference_lsoa21_ward25_lad25') }} la on la.LSOA21_CD = p.LSOA_CODE_21
)

SELECT distinct * FROM uptake_with_demographics
ORDER BY programme_type, campaign_id, practice_code, person_id