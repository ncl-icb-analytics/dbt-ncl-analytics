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
- COVID Autumn 2024, COVID Spring 2025, COVID Autumn 2025

Flu Campaigns: 
- Flu 2023-24, Flu 2024-25, Flu 2025-26

Usage:
- Primary table for COVID and Flu Dashboard in PowerBI/Tableau
- Filter by programme_type for programme-specific dashboards
- Use campaign_id for specific campaign analysis
- Demographic breakdowns for equity analysis
*/

WITH uptake_with_demographics AS (
    SELECT 
        -- Programme and campaign identifiers
        u.programme_type,
        u.campaign_id,
        u.campaign_year,
        u.campaign_season,
        u.person_id,
        
        -- Demographics from dim_person_demographics
        d.is_active,
        d.gender,
        d.age,
        d.age_band_5y,
        d.age_band_10y,
        d.age_band_nhs,
        d.ethnicity_category,
        d.ethnicity_subcategory,
        d.ethnicity_granular,
        d.main_language,
        d.language_type,
        d.interpreter_needed,
        d.interpreter_type,
        
        -- Geographic and deprivation (residence-based)
        d.imd_quintile_19,
        d.imd_decile_19,
        d.lsoa_code_21 as lsoa_code,
        d.lsoa_name_21 as lsoa_name,
        d.borough_resident,
        d.neighbourhood_resident,
        d.icb_code_resident,
        d.icb_resident,

        -- Practice information (registration-based)
        d.practice_code,
        d.practice_name,
        d.pcn_code,
        d.pcn_name,
        d.borough_registered,
        d.neighbourhood_registered,
        
        -- School information from dim_person_age (now handles NULL for non-school ages upstream)
        pa.age_school_stage AS school_year,
        pa.is_primary_school_age,
        pa.is_secondary_school_age,

        -- Flu vaccination setting (Early Years at GP, School-based for Reception-Year 11)
        CASE
            WHEN u.programme_type = 'FLU' THEN
                CASE
                    WHEN pa.age < 4 THEN 'Early Years (GP)'  -- Pre-school and Nursery ages
                    WHEN pa.age_school_stage IN ('Reception', 'Year 1', 'Year 2', 'Year 3', 'Year 4',
                                                  'Year 5', 'Year 6', 'Year 7', 'Year 8', 'Year 9',
                                                  'Year 10', 'Year 11') THEN 'School-based'
                    ELSE NULL  -- Year 12, Year 13, and older
                END
            ELSE NULL
        END AS flu_vaccination_setting,

        -- Housebound status from dim_person_housebound_status
        COALESCE(hs.is_housebound, FALSE) AS is_housebound,
        
        -- Eligibility information from uptake facts
        u.is_eligible,
        u.campaign_category,
        u.risk_group,
        u.eligibility_reason,
        u.rule_type,
        
        -- Vaccination information from uptake facts
        u.vaccination_status,
        u.vaccination_date,
        u.vaccination_status_reason,
        u.vaccinated_despite_ineligible,
        
        -- Uptake flags and metrics from uptake facts
        u.vaccinated,
        u.declined,
        u.eligible_no_record,
        u.uptake_category,
        u.days_to_vaccination,
        
        -- Programme-specific fields
        u.laiv_given,
        
        -- Campaign dates
        u.campaign_start_date,
        u.campaign_reference_date,
        u.audit_end_date,
        u.created_at
        
    FROM {{ ref('fct_covid_flu_uptake') }} u
    LEFT JOIN {{ ref('dim_person_demographics') }} d
        ON u.person_id = d.person_id
    LEFT JOIN {{ ref('dim_person_age') }} pa
        ON u.person_id = pa.person_id
    LEFT JOIN {{ ref('dim_person_housebound_status') }} hs
        ON u.person_id = hs.person_id
)

SELECT * FROM uptake_with_demographics
ORDER BY programme_type, campaign_id, practice_code, person_id