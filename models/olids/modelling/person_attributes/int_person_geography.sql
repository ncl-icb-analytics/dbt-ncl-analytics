{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Person Geography
Comprehensive geographic mapping for persons including postcode hash, LSOA, borough, neighbourhood and IMD.
Uses person_id directly from patient_address table for accurate matching.
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
        pa.postcode_hash,
        pa.start_date,
        pa.end_date
    FROM {{ ref('stg_olids_patient_address') }} pa
    -- Only include addresses for valid persons
    INNER JOIN valid_persons vp
        ON pa.person_id = vp.person_id
    WHERE pa.person_id IS NOT NULL
        AND pa.postcode_hash IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pa.person_id
        ORDER BY
            CASE WHEN pa.end_date IS NULL THEN 0 ELSE 1 END,  -- Active addresses first
            pa.start_date DESC NULLS LAST,
            pa.lds_datetime_data_acquired DESC NULLS LAST
    ) = 1
),

postcode_geography AS (
    -- Get the latest geography data for each postcode hash
    SELECT
        postcode_hash,
        primary_care_organisation,
        local_authority_organisation,
        yr_2011_lsoa,
        yr_2011_msoa,
        yr_2021_lsoa,
        yr_2021_msoa
    FROM {{ ref('stg_olids_postcode_hash') }}
    WHERE is_latest = TRUE
        AND postcode_hash IS NOT NULL
),

imd_reference AS (
    -- Get IMD 2019 data (based on 2011 LSOA codes)
    SELECT
        lsoacode,
        imddecile
    FROM {{ ref('stg_reference_imd2019') }}
    WHERE lsoacode IS NOT NULL
        AND imddecile IS NOT NULL
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

lsoa_names AS (
    -- Get LSOA names for 2021 codes
    SELECT DISTINCT
        lsoa21_cd,
        lsoa21_nm
    FROM {{ ref('stg_reference_lsoa2011_lsoa2021') }}
    WHERE lsoa21_cd IS NOT NULL
        AND lsoa21_nm IS NOT NULL
)

SELECT
    ca.person_id,
    ca.postcode_hash,
    ca.start_date as address_start_date,
    ca.end_date as address_end_date,
    CASE WHEN ca.end_date IS NULL THEN TRUE ELSE FALSE END as is_current_address,

    -- Geographic identifiers
    pg.primary_care_organisation,
    pco_org.organisation_name as icb_resident,
    pg.local_authority_organisation,

    -- Borough resident - strip 'London Borough of' prefix from local authority organisation name
    CASE
        WHEN la_org.organisation_name LIKE 'London Borough of %'
            THEN TRIM(SUBSTRING(la_org.organisation_name, 19))
        ELSE la_org.organisation_name
    END as borough_resident,

    -- 2011 Census geography
    pg.yr_2011_lsoa as lsoa_code_11,
    pg.yr_2011_msoa as msoa_code_11,

    -- 2021 Census geography
    pg.yr_2021_lsoa as lsoa_code_21,
    ln.lsoa21_nm as lsoa_name_21,
    pg.yr_2021_msoa as msoa_code_21,

    -- NCL Neighbourhood (from 2021 LSOA)
    nr.neighbourhood_name as neighbourhood_resident,

    -- IMD 2019 data (matched on 2011 LSOA as IMD2019 uses 2011 boundaries)
    imd.imddecile as imd_decile_19,

    -- IMD Quintile calculation
    CASE
        WHEN imd.imddecile IN (1, 2) THEN 'Most Deprived'
        WHEN imd.imddecile IN (3, 4) THEN 'Second Most Deprived'
        WHEN imd.imddecile IN (5, 6) THEN 'Third Most Deprived'
        WHEN imd.imddecile IN (7, 8) THEN 'Second Least Deprived'
        WHEN imd.imddecile IN (9, 10) THEN 'Least Deprived'
        ELSE NULL
    END AS imd_quintile_19

FROM current_addresses ca
LEFT JOIN postcode_geography pg
    ON ca.postcode_hash = pg.postcode_hash
LEFT JOIN imd_reference imd
    ON pg.yr_2011_lsoa = imd.lsoacode
LEFT JOIN neighbourhood_reference nr
    ON pg.yr_2021_lsoa = nr.lsoa_2021_code
LEFT JOIN lsoa_names ln
    ON pg.yr_2021_lsoa = ln.lsoa21_cd
-- Join organisation names for primary care organisation
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} pco_org
    ON pg.primary_care_organisation = pco_org.organisation_code
    AND (pco_org.end_date IS NULL OR pco_org.end_date >= CURRENT_DATE())
-- Join organisation names for local authority/borough
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} la_org
    ON pg.local_authority_organisation = la_org.organisation_code
    AND (la_org.end_date IS NULL OR la_org.end_date >= CURRENT_DATE())