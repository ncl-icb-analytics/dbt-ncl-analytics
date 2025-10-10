{{
    config(
        materialized='table'
    )
}}

/*
London Area Identification using LSOA Reference Data
Uses the LSOA21_WARD25_LAD25 reference table which has a RESIDENT_FLAG
that clearly identifies NCL, Other London, and Outside London areas.
*/

SELECT DISTINCT
    lad25_cd,
    lad25_nm,

    -- London classification based on resident_flag
    CASE
        WHEN resident_flag = 'NCL' THEN 'NCL'
        WHEN resident_flag = 'Other London' THEN 'Other London'
        WHEN resident_flag = 'Outside London' THEN 'Outside London'
        ELSE 'Unknown'
    END AS london_classification,

    -- Boolean flags for easier filtering
    CASE WHEN resident_flag IN ('NCL', 'Other London') THEN TRUE ELSE FALSE END AS is_london_area,
    CASE WHEN resident_flag = 'NCL' THEN TRUE ELSE FALSE END AS is_ncl_area,

    -- Borough name (for London areas only)
    CASE
        WHEN resident_flag IN ('NCL', 'Other London') THEN lad25_nm
        ELSE NULL
    END AS borough_name

FROM {{ ref('stg_reference_lsoa21_ward25_lad25') }}
WHERE lad25_cd IS NOT NULL
    AND lad25_nm IS NOT NULL