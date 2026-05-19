{{
    config(
        materialized='table',
        tags=['dimension', 'practice', 'organisation'],
        cluster_by=['practice_code'])
}}

/*
Active Practice Dimension
Filtered variant of dim_practice restricted to operationally active practices
(ODS status = Active and end date NULL or in the future).
*/

SELECT *
FROM {{ ref('dim_practice') }}
WHERE is_active_practice = TRUE
