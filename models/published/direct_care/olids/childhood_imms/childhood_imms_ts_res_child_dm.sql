{{
    config(
        materialized='view',
        tags=['childhood_imms']
    )
}}

SELECT *
FROM (
SELECT * 
FROM {{ ref('int_childhood_imms_ts_agg_res_age_1')}}
UNION 
SELECT *
FROM {{ ref('int_childhood_imms_ts_agg_res_age_2')}}
UNION
SELECT *
FROM {{ ref('int_childhood_imms_ts_agg_res_age_5')}}
)p