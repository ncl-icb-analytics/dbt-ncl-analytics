{{
    config(
        materialized='view',
        tags=['childhood_imms']
    )
}}

SELECT *
FROM {{ ref('childhood_imms_time_series_adolescent_res_dm_tab')}}