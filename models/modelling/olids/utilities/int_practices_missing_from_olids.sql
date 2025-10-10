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
    l.practicecode as practice_code,
    l.practicename as practice_name,
    l.localauthority as local_authority,
    l.practiceneighbourhood as practice_neighbourhood
FROM {{ ref('stg_reference_practice_neighbourhood_lookup') }} l
LEFT JOIN {{ ref('dim_person_demographics') }} d
    ON d.practice_code = l.practicecode
WHERE d.practice_code IS NULL
