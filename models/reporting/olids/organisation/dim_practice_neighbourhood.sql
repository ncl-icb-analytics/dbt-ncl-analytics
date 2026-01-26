{{
    config(
        materialized='table',
        tags=['dimension', 'practice', 'geography'],
        cluster_by=['practice_code'])
}}

/*
Practice Neighbourhood Dimension
Provides geographic context for GP practices including local authority and neighbourhood classification.
Note: Working with dummy data so geographic information may be limited/placeholder.
*/

SELECT
    gp_practice_code AS practice_code,
    practice_name AS practice_name,
    borough AS local_authority,
    neighbourhood_name AS neighbourhood_registered
FROM {{ ref('stg_reference_lookup_ncl_gp_practice') }}
WHERE gp_practice_code IS NOT NULL
