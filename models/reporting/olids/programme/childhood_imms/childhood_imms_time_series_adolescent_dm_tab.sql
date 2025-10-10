{{
    config(
        materialized='table',
        tags=['childhood_imms']
    )
}}

SELECT *
FROM (
SELECT * 
FROM {{ ref('int_childhood_imms_ts_agg_age_11')}} 
UNION 
SELECT *
FROM {{ ref('int_childhood_imms_ts_agg_age_16')}} 

)p