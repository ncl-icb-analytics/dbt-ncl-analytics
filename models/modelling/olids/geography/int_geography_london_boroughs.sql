{{
    config(
        materialized='table'
    )
}}

/*
London Borough Identification and Cleaning
Identifies London boroughs from ICBs and local authorities, providing cleaned borough names.
*/

WITH icb_organisations AS (
    -- Get all ICB organisations (primary care organisations) with cleaned names
    SELECT DISTINCT
        organisation_code,
        organisation_name AS organisation_name_original,
        -- Remove organisation code from the end of the name if present
        CASE
            WHEN organisation_name LIKE '% - ' || organisation_code
                THEN TRIM(REGEXP_REPLACE(organisation_name, ' - ' || organisation_code || '$', ''))
            -- Handle other code patterns like "NHS North West London ICB - W2U3Z"
            WHEN organisation_name RLIKE '.* - [A-Z0-9]+$'
                THEN TRIM(REGEXP_REPLACE(organisation_name, ' - [A-Z0-9]+$', ''))
            ELSE organisation_name
        END AS organisation_name_clean,
        CASE
            WHEN UPPER(organisation_name) LIKE '%LONDON%' THEN TRUE
            ELSE FALSE
        END AS is_london_icb
    FROM {{ ref('stg_dictionary_dbo_organisation') }}
    WHERE organisation_code IS NOT NULL
        AND organisation_name IS NOT NULL
        AND (end_date IS NULL OR end_date >= CURRENT_DATE())
),

local_authority_organisations AS (
    -- Get all local authority organisations with cleaned names
    SELECT DISTINCT
        organisation_code,
        organisation_name AS organisation_name_original,
        -- Clean borough names by removing prefixes
        CASE
            WHEN organisation_name LIKE 'London Borough of %'
                THEN TRIM(SUBSTRING(organisation_name, 19))
            WHEN organisation_name LIKE 'Royal Borough of %'
                THEN TRIM(SUBSTRING(organisation_name, 18))
            WHEN organisation_name = 'City of Westminster'
                THEN 'Westminster'
            WHEN organisation_name = 'City of London'
                THEN 'City of London'
            ELSE organisation_name
        END AS organisation_name_clean,
        CASE
            WHEN organisation_name LIKE 'London Borough of %'
              OR organisation_name LIKE 'Royal Borough of %'
              OR organisation_name IN ('City of Westminster', 'City of London')
            THEN TRUE
            ELSE FALSE
        END AS is_london_borough
    FROM {{ ref('stg_dictionary_dbo_organisation') }}
    WHERE organisation_code IS NOT NULL
        AND organisation_name IS NOT NULL
        AND (end_date IS NULL OR end_date >= CURRENT_DATE())
)

-- ICB organisations with London flags
SELECT
    organisation_code AS icb_code,
    organisation_name_original AS icb_name_original,
    organisation_name_clean AS icb_name_clean,
    is_london_icb,
    NULL AS local_authority_code,
    NULL AS local_authority_name_original,
    NULL AS local_authority_name_clean,
    NULL AS is_london_borough,
    is_london_icb AS is_london_geography
FROM icb_organisations

UNION ALL

-- Local Authority organisations with London flags
SELECT
    NULL AS icb_code,
    NULL AS icb_name_original,
    NULL AS icb_name_clean,
    NULL AS is_london_icb,
    organisation_code AS local_authority_code,
    organisation_name_original AS local_authority_name_original,
    organisation_name_clean AS local_authority_name_clean,
    is_london_borough,
    is_london_borough AS is_london_geography
FROM local_authority_organisations