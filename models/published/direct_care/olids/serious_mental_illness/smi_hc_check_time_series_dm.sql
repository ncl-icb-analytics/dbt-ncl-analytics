{{
    config(
        materialized='view',
        tags=['smi_registry']
    )
}}

select * 
FROM {{ ref('int_smi_hc_check_time_series') }}