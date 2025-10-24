{{
    config(
        materialized='view',
        tags=['childhood_imms']
    )
}}

SELECT *
FROM (
select * 
FROM {{ ref('int_childhood_imms_eoy_count_agg_age_1') }}
UNION
select * 
FROM {{ ref('int_childhood_imms_eoy_count_agg_age_2') }}
UNION
select * 
FROM {{ ref('int_childhood_imms_eoy_count_agg_age_5') }}
)p