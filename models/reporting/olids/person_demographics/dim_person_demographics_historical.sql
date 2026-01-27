{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'demographics', 'historical', 'scd2'],
        cluster_by=['person_id', 'effective_start_date'])
}}

/*
Historical Person Demographics - SCD Type 2

Tracks changes to demographic attributes over time using SCD-2 pattern.
One row per person per change period (not per month).

Key Features:

• SCD-2 structure with effective_start_date, effective_end_date, is_current

• Tracks changes in: practice registration, ethnicity, address/geography

• No age fields (age is derived, not an attribute - calculate in consuming models)

• Change detection: new period created when practice OR ethnicity OR address changes

Data Quality:

• Only includes persons with valid birth dates

• Only includes persons with registration history

For monthly snapshots, use person_month_analysis_base.
For current state only, use dim_person_demographics.
*/

WITH person_registrations AS (
    -- Get all registration periods with practice details
    SELECT
        person_id,
        practice_ods_code as practice_code,
        practice_name,
        registration_start_date,
        registration_end_date,
        effective_end_date,
        is_current_registration
    FROM {{ ref('int_patient_registrations') }}
),

ethnicity_changes AS (
    -- Get ethnicity changes over time
    SELECT
        person_id,
        ethnicity_category,
        ethnicity_subcategory,
        ethnicity_granular,
        category_sort as ethnicity_category_sort,
        display_sort_key as ethnicity_display_sort_key,
        clinical_effective_date as ethnicity_effective_date,
        preference_rank
    FROM {{ ref('int_ethnicity_all') }}
),

address_changes AS (
    -- Get address changes over time (from person geography which uses SCD logic)
    SELECT
        person_id,
        postcode_hash,
        address_start_date,
        address_end_date,
        primary_care_organisation as icb_code_resident,
        icb_resident,
        local_authority_code,
        local_authority_name,
        borough_resident,
        is_london_resident,
        london_classification,
        lsoa_code_21,
        lsoa_name_21,
        ward_code,
        ward_name,
        imd_decile_19,
        imd_quintile_19,
        imd_quintile_numeric_19,
        imd_decile_25,
        imd_quintile_25,
        imd_quintile_numeric_25,
        neighbourhood_resident
    FROM {{ ref('int_person_geography') }}
),

all_change_dates AS (
    -- Combine all dates where any attribute changed
    SELECT DISTINCT person_id, registration_start_date as change_date
    FROM person_registrations
    WHERE registration_start_date IS NOT NULL

    UNION

    SELECT DISTINCT person_id, registration_end_date as change_date
    FROM person_registrations
    WHERE registration_end_date IS NOT NULL

    UNION

    SELECT DISTINCT person_id, ethnicity_effective_date as change_date
    FROM ethnicity_changes
    WHERE ethnicity_effective_date IS NOT NULL

    UNION

    SELECT DISTINCT person_id, address_start_date as change_date
    FROM address_changes
    WHERE address_start_date IS NOT NULL

    UNION

    SELECT DISTINCT person_id, address_end_date as change_date
    FROM address_changes
    WHERE address_end_date IS NOT NULL
),

change_periods AS (
    -- Create periods between change dates
    SELECT
        person_id,
        change_date as effective_start_date,
        LEAD(change_date) OVER (
            PARTITION BY person_id
            ORDER BY change_date
        ) as effective_end_date,
        ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY change_date
        ) as period_sequence
    FROM all_change_dates
),

registrations_for_periods AS (
    -- Get latest registration active for each period
    -- Uses effective_end_date (accounts for death) rather than registration_end_date
    SELECT
        cp.person_id,
        cp.effective_start_date,
        pr.practice_code,
        pr.practice_name,
        pr.registration_start_date,
        pr.registration_end_date,
        pr.effective_end_date as registration_effective_end_date,
        pr.is_current_registration,
        ROW_NUMBER() OVER (
            PARTITION BY cp.person_id, cp.effective_start_date
            ORDER BY pr.is_current_registration DESC, pr.registration_start_date DESC
        ) as rn
    FROM change_periods cp
    LEFT JOIN person_registrations pr
        ON cp.person_id = pr.person_id
        AND pr.registration_start_date <= cp.effective_start_date
        AND (pr.effective_end_date IS NULL OR pr.effective_end_date >= cp.effective_start_date)
),

ethnicity_for_periods AS (
    -- Get latest ethnicity known at each period start
    SELECT
        cp.person_id,
        cp.effective_start_date,
        ec.ethnicity_category,
        ec.ethnicity_subcategory,
        ec.ethnicity_granular,
        ec.ethnicity_category_sort,
        ec.ethnicity_display_sort_key,
        ROW_NUMBER() OVER (
            PARTITION BY cp.person_id, cp.effective_start_date
            ORDER BY ec.ethnicity_effective_date DESC, ec.preference_rank ASC
        ) as rn
    FROM change_periods cp
    LEFT JOIN ethnicity_changes ec
        ON cp.person_id = ec.person_id
        AND ec.ethnicity_effective_date <= cp.effective_start_date
),

address_for_periods AS (
    -- Get current address for each person (address SCD dates not yet populated)
    -- TODO: Once address_start_date/address_end_date are populated, use temporal join
    SELECT
        cp.person_id,
        cp.effective_start_date,
        ac.postcode_hash,
        ac.icb_code_resident,
        ac.icb_resident,
        ac.local_authority_code,
        ac.local_authority_name,
        ac.borough_resident,
        ac.is_london_resident,
        ac.london_classification,
        ac.lsoa_code_21,
        ac.lsoa_name_21,
        ac.ward_code,
        ac.ward_name,
        ac.imd_decile_19,
        ac.imd_quintile_19,
        ac.imd_quintile_numeric_19,
        ac.imd_decile_25,
        ac.imd_quintile_25,
        ac.imd_quintile_numeric_25,
        ac.neighbourhood_resident
    FROM change_periods cp
    LEFT JOIN address_changes ac
        ON cp.person_id = ac.person_id
),

periods_with_attributes AS (
    -- Combine all attributes for each period
    SELECT
        cp.person_id,
        cp.effective_start_date,
        cp.effective_end_date,
        cp.period_sequence,
        CASE WHEN cp.effective_end_date IS NULL THEN TRUE ELSE FALSE END as is_current,

        -- Registration
        pr.practice_code,
        pr.practice_name,
        pr.registration_start_date,
        pr.registration_end_date,
        pr.registration_effective_end_date,
        pr.is_current_registration,

        -- Ethnicity
        ec.ethnicity_category,
        ec.ethnicity_subcategory,
        ec.ethnicity_granular,
        ec.ethnicity_category_sort,
        ec.ethnicity_display_sort_key,

        -- Address
        ac.postcode_hash,
        ac.icb_code_resident,
        ac.icb_resident,
        ac.local_authority_code,
        ac.local_authority_name,
        ac.borough_resident,
        ac.is_london_resident,
        ac.london_classification,
        ac.lsoa_code_21,
        ac.lsoa_name_21,
        ac.ward_code,
        ac.ward_name,
        ac.imd_decile_19,
        ac.imd_quintile_19,
        ac.imd_quintile_numeric_19,
        ac.imd_decile_25,
        ac.imd_quintile_25,
        ac.imd_quintile_numeric_25,
        ac.neighbourhood_resident

    FROM change_periods cp
    LEFT JOIN registrations_for_periods pr
        ON cp.person_id = pr.person_id
        AND cp.effective_start_date = pr.effective_start_date
        AND pr.rn = 1
    LEFT JOIN ethnicity_for_periods ec
        ON cp.person_id = ec.person_id
        AND cp.effective_start_date = ec.effective_start_date
        AND ec.rn = 1
    LEFT JOIN address_for_periods ac
        ON cp.person_id = ac.person_id
        AND cp.effective_start_date = ac.effective_start_date
)

SELECT
    -- Core identifiers
    pwa.person_id,
    bd.sk_patient_id,

    -- SCD-2 fields
    pwa.effective_start_date,
    pwa.effective_end_date,
    pwa.is_current,
    pwa.period_sequence,

    -- Status flags (temporal: was the registration active during this period?)
    CASE
        WHEN pwa.practice_code IS NULL THEN FALSE
        WHEN pwa.registration_effective_end_date IS NULL THEN TRUE
        WHEN pwa.registration_effective_end_date >= pwa.effective_start_date THEN TRUE
        ELSE FALSE
    END AS is_active,
    bd.is_deceased,
    bd.is_dummy_patient,
    CASE
        WHEN pwa.practice_code IS NULL THEN 'No registration history'
        WHEN bd.is_deceased
            AND bd.death_date_approx IS NOT NULL
            AND pwa.effective_start_date >= bd.death_date_approx
            THEN 'Deceased'
        WHEN pwa.registration_effective_end_date IS NOT NULL
            AND pwa.effective_start_date >= pwa.registration_effective_end_date
            THEN 'Registration ended'
        ELSE NULL
    END AS inactive_reason,

    -- Birth and death information (no age - calculate in consuming models)
    bd.birth_year,
    bd.birth_month,
    bd.birth_date_approx,
    CASE
        WHEN bd.birth_year IS NOT NULL AND bd.birth_month IS NOT NULL
            THEN LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1))
        ELSE NULL
    END AS birth_date_approx_end_of_month,
    bd.death_year,
    bd.death_month,
    bd.death_date_approx,

    -- Gender
    COALESCE(gender.gender, 'Unknown') AS gender,

    -- Ethnicity
    COALESCE(pwa.ethnicity_category, 'Unknown') AS ethnicity_category,
    COALESCE(pwa.ethnicity_subcategory, 'Unknown') AS ethnicity_subcategory,
    COALESCE(pwa.ethnicity_granular, 'Unknown') AS ethnicity_granular,
    pwa.ethnicity_category_sort,
    pwa.ethnicity_display_sort_key,

    -- Language
    lang.language AS main_language,
    lang.language_type,
    lang.interpreter_type,
    COALESCE(lang.interpreter_needed, FALSE) AS interpreter_needed,

    -- Practice registration
    pwa.practice_code,
    pwa.practice_name,
    pwa.registration_start_date,
    pwa.registration_end_date,

    -- PCN Information
    dp.pcn_code,
    dp.pcn_name,
    dp.pcn_name_with_borough,

    -- ICB Information
    dp.stp_code AS icb_code,
    dp.stp_name AS icb_name,

    -- Geographic Information (practice-based)
    dp.borough_registered,
    dp.practice_postcode_dict AS practice_postcode,
    dp.practice_lsoa,
    dp.practice_msoa,
    dp.practice_latitude,
    dp.practice_longitude,
    nbhd.neighbourhood_registered,

    -- Address Information
    pwa.postcode_hash,
    NULL AS uprn_hash,
    NULL::VARCHAR AS household_id,

    -- Geographic Data (residence-based)
    pwa.icb_code_resident,
    pwa.icb_resident,
    pwa.local_authority_code,
    pwa.local_authority_name,
    pwa.borough_resident,
    pwa.is_london_resident,
    pwa.london_classification,
    pwa.lsoa_code_21,
    pwa.lsoa_name_21,
    pwa.ward_code,
    pwa.ward_name,
    pwa.imd_decile_19,
    pwa.imd_quintile_19,
    pwa.imd_quintile_numeric_19,
    pwa.imd_decile_25,
    pwa.imd_quintile_25,
    pwa.imd_quintile_numeric_25,
    pwa.neighbourhood_resident

FROM periods_with_attributes pwa

-- Join birth/death
INNER JOIN {{ ref('dim_person_birth_death') }} bd
    ON pwa.person_id = bd.person_id

-- Join gender
LEFT JOIN {{ ref('dim_person_gender') }} gender
    ON pwa.person_id = gender.person_id

-- Join language
LEFT JOIN {{ ref('dim_person_main_language') }} lang
    ON pwa.person_id = lang.person_id

-- Join practice details
LEFT JOIN (
    SELECT
        practice_code,
        practice_name,
        pcn_code,
        pcn_name,
        pcn_name_with_borough,
        borough_registered,
        practice_postcode_dict,
        practice_lsoa,
        practice_msoa,
        practice_latitude,
        practice_longitude,
        stp_code,
        stp_name
    FROM {{ ref('dim_practice') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY practice_code ORDER BY practice_type_desc NULLS LAST) = 1
) dp ON pwa.practice_code = dp.practice_code

-- Join practice neighbourhood
LEFT JOIN {{ ref('dim_practice_neighbourhood') }} nbhd
    ON pwa.practice_code = nbhd.practice_code

-- Filter: Must have birth date AND registration history
WHERE bd.birth_date_approx IS NOT NULL
  AND pwa.practice_code IS NOT NULL

ORDER BY pwa.person_id, pwa.effective_start_date
