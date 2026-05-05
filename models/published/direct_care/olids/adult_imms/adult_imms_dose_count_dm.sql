{{
    config(
        materialized='view',
        tags=['adult_imms']
    )
}}

select * 
FROM {{ ref('int_adult_imms_dose_count_agg') }}