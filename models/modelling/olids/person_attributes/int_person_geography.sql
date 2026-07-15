{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Person Geography
Geographic mapping for persons including postcode, LSOA, borough, neighbourhood and IMD.
Uses refactored intermediate geography tables for clean separation of concerns.
All geographic joins are LEFT JOINs as not everyone lives in London/NCL boundaries.
*/

WITH valid_persons AS (
    -- Get all valid persons from the unique mapping table
    SELECT DISTINCT person_id
    FROM {{ ref('int_patient_person_unique') }}
),

current_addresses AS (
    -- Get the latest address for each person using SCD2 logic
    SELECT
        pa.person_id,
        pa.postcode,
        pa.start_date,
        pa.end_date
    FROM {{ ref('stg_olids_patient_address') }} pa
    -- Only include addresses for valid persons
    INNER JOIN valid_persons vp
        ON pa.person_id = vp.person_id
    WHERE pa.person_id IS NOT NULL
        AND pa.postcode IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pa.person_id
        ORDER BY
            CASE WHEN pa.end_date IS NULL THEN 0 ELSE 1 END,  -- Active addresses first
            pa.start_date DESC NULLS LAST,
            pa.lds_transform_datetime DESC NULLS LAST
    ) = 1
),

postcode_geography AS (
    -- REVIEW: Dictionary.dbo.Postcode replaces the removed address-to-hash join.
    SELECT
        postcode_no_space,
        primary_care_organisation,
        local_authority_district_unitary_authority as local_authority_organisation,
        yr2011_lsoa as yr_2011_lsoa,
        yr2011_msoa as yr_2011_msoa,
        lsoa as yr_2021_lsoa, -- REVIEW: generic current LSOA is treated as the 2021 code.
        msoa as yr_2021_msoa -- REVIEW: generic current MSOA is treated as the 2021 code.
    FROM {{ ref('stg_dictionary_dbo_postcode') }}
    WHERE postcode_no_space IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY postcode_no_space
        ORDER BY date_of_termination DESC NULLS FIRST, last_updated DESC NULLS LAST
    ) = 1
),

neighbourhood_reference AS (
    -- Get NCL neighbourhood data (based on 2021 LSOA codes)
    SELECT DISTINCT
        lsoa_2021_code,
        neighbourhood_name
    FROM {{ ref('stg_reference_ncl_neighbourhood_lsoa_2021') }}
    WHERE lsoa_2021_code IS NOT NULL
        AND neighbourhood_name IS NOT NULL
),

london_areas AS (
    -- Get London area classification from LSOA reference
    SELECT DISTINCT
        lsoa21_cd,
        lad25_cd,
        lad25_nm,
        wd25_cd,
        wd25_nm,
        resident_flag,
        CASE WHEN resident_flag IN ('NCL', 'Other London') THEN TRUE ELSE FALSE END AS is_london_resident
    FROM {{ ref('stg_reference_lsoa21_ward25_lad25') }}
    WHERE lsoa21_cd IS NOT NULL
)

SELECT
    ca.person_id,
    ca.postcode, -- REVIEW: plain postcode replaces the removed postcode_hash output.
    ca.start_date as address_start_date,
    ca.end_date as address_end_date,
    CASE WHEN ca.end_date IS NULL THEN TRUE ELSE FALSE END as is_current_address,

    -- Geographic identifiers
    pg.primary_care_organisation,
    pco_map.name as icb_resident,

    -- Local authority information - use LAD from reference data (most reliable)
    COALESCE(la.lad25_cd, pg.local_authority_organisation) as local_authority_code,
    COALESCE(la.lad25_nm, la_map.name) as local_authority_name,

    -- Ward information from LSOA reference
    la.wd25_cd as ward_code,
    la.wd25_nm as ward_name,

    -- Borough resident - only populated for London areas
    CASE
        WHEN la.is_london_resident = TRUE THEN la.lad25_nm
        ELSE NULL
    END as borough_resident,

    -- 2011 Census geography
    pg.yr_2011_lsoa as lsoa_code_11,
    pg.yr_2011_msoa as msoa_code_11,

    -- 2021 Census geography
    pg.yr_2021_lsoa as lsoa_code_21,
    lsoa_map.name as lsoa_name_21,
    pg.yr_2021_msoa as msoa_code_21,

    -- NCL Neighbourhood (from 2021 LSOA)
    nr.neighbourhood_name as neighbourhood_resident,

    -- IMD 2019 data from dedicated IMD model
    imd.imd_decile_19,
    imd.imd_quintile_19,
    imd.imd_quintile_numeric_19,
    imd.is_most_deprived_20pct,

    -- IMD 2025 data from dedicated IMD model
    imd25.imd_decile_25,
    imd25.imd_quintile_25,
    imd25.imd_quintile_numeric_25,
    imd25.is_most_deprived_20pct_25,

    -- London resident flag from LSOA reference (more reliable)
    COALESCE(la.is_london_resident, FALSE) as is_london_resident,

    -- London classification details
    la.resident_flag as london_classification

FROM current_addresses ca
LEFT JOIN postcode_geography pg
    ON REGEXP_REPLACE(UPPER(TRIM(ca.postcode)), '\\s+', '') = pg.postcode_no_space

-- Join geographic mappings for names
LEFT JOIN {{ ref('int_geography_mappings') }} pco_map
    ON pg.primary_care_organisation = pco_map.code
    AND pco_map.mapping_type = 'PRIMARY_CARE'
LEFT JOIN {{ ref('int_geography_mappings') }} la_map
    ON pg.local_authority_organisation = la_map.code
    AND la_map.mapping_type = 'LOCAL_AUTHORITY'
LEFT JOIN {{ ref('int_geography_mappings') }} lsoa_map
    ON pg.yr_2021_lsoa = lsoa_map.code
    AND lsoa_map.mapping_type = 'LSOA_2021'

-- Join London area information via LSOA
LEFT JOIN london_areas la
    ON pg.yr_2021_lsoa = la.lsoa21_cd

-- Join IMD 2019 information (via 2011 LSOA)
LEFT JOIN {{ ref('int_geography_lsoa_2011') }} imd
    ON pg.yr_2011_lsoa = imd.lsoa_code_2011

-- Join IMD 2025 information (via 2021 LSOA)
LEFT JOIN {{ ref('int_geography_lsoa_2021') }} imd25
    ON pg.yr_2021_lsoa = imd25.lsoa_code_2021

-- Join neighbourhood reference
LEFT JOIN neighbourhood_reference nr
    ON pg.yr_2021_lsoa = nr.lsoa_2021_code
