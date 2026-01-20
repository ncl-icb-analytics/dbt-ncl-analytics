{{
    config(
        materialized='view',
        tags=['childhood_imms']
    )
}}


select * 
FROM {{ ref('int_childhood_imms_dose_count_agg_child') }}