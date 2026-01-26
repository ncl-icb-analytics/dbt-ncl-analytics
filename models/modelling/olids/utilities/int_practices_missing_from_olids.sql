{{
    config(
        materialized='table',
        tags=['data_quality', 'utilities', 'olids']
    )
}}

/*
Practices Missing from OLIDS

Identifies practices present in the reference practice lookup but with no patients
registered in OLIDS demographics.

This may indicate:
- New practices not yet in OLIDS
- Practices with data feed issues
- Closed practices still in reference data
- Configuration or mapping problems

Uses the practice neighbourhood lookup as the master list of expected practices.
*/

SELECT
    l.gp_practice_code as practice_code,
    l.practice_name,
    l.borough as local_authority,
    l.neighbourhood_name as practice_neighbourhood
FROM {{ ref('stg_reference_lookup_ncl_gp_practice') }} l
LEFT JOIN {{ ref('dim_person_demographics') }} d
    ON d.practice_code = l.gp_practice_code
WHERE d.practice_code IS NULL
