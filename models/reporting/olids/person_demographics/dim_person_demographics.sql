{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'demographics', 'current_state'],
        cluster_by=['person_id'])
}}

/*
Current Person Demographics Dimension Table

Provides current demographic snapshot for ALL persons with registration history.
Built as a thin wrapper over dim_person_demographics_historical, selecting only current periods.

Key Features:

• One row per person (current demographics only)

• Includes ALL persons with registration history (active and inactive)

• Age calculated dynamically for today's date

• is_active flag shows current registration status

• Practice details show latest/current registration

Data Quality Filters:

• Excludes persons without birth dates (required for age)

• Excludes persons without any registration history

For historical analysis, use dim_person_demographics_historical.
For monthly snapshots, use person_month_analysis_base.
*/

SELECT
    -- Core Identifiers
    hist.person_id,
    hist.sk_patient_id,

    -- Status Flags
    hist.is_active,
    hist.is_deceased,
    hist.is_dummy_patient,
    hist.inactive_reason,

    -- Basic Demographics
    hist.birth_year,
    hist.birth_date_approx,
    hist.birth_date_approx_end_of_month,
    hist.death_year,
    hist.death_date_approx,

    -- Age calculations (current as of today or death date)
    {{ calculate_age_attributes(
        birth_date_field='hist.birth_date_approx',
        reference_date_field='CURRENT_DATE',
        birth_year_field='hist.birth_year',
        birth_month_field='hist.birth_month',
        is_deceased_field='hist.is_deceased',
        death_date_field='hist.death_date_approx'
    ) }},

    -- Sex
    hist.sex,

    -- Ethnicity
    hist.ethnicity_category,
    hist.ethnicity_subcategory,
    hist.ethnicity_granular,
    hist.ethnicity_category_sort,
    hist.ethnicity_display_sort_key,

    -- Language and Communication
    hist.main_language,
    hist.language_type,
    hist.interpreter_type,
    hist.interpreter_needed,

    -- Practice Registration (current or latest)
    hist.practice_code,
    hist.practice_name,
    hist.registration_start_date,
    hist.registration_end_date,

    -- PCN Information
    hist.pcn_code,
    hist.pcn_name,
    hist.pcn_name_with_borough,

    -- ICB Information
    hist.icb_code,
    hist.icb_name,

    -- Geographic Information
    hist.borough_registered,
    hist.practice_postcode,
    hist.practice_lsoa,
    hist.practice_msoa,
    hist.practice_latitude,
    hist.practice_longitude,
    hist.neighbourhood_registered,

    -- Address Information
    hist.postcode_hash,
    hist.uprn_hash,
    hist.household_id,

    -- Geographic Data from person postcode mapping (residence-based)
    hist.icb_code_resident,
    hist.icb_resident,
    hist.local_authority_code,
    hist.local_authority_name,
    hist.borough_resident,
    hist.is_london_resident,
    hist.london_classification,
    hist.lsoa_code_21,
    hist.lsoa_name_21,
    hist.ward_code,
    hist.ward_name,
    hist.imd_decile_19,
    hist.imd_quintile_19,
    hist.imd_quintile_numeric_19,
    hist.neighbourhood_resident

FROM {{ ref('dim_person_demographics_historical') }} hist

WHERE hist.is_current = TRUE

ORDER BY hist.person_id
