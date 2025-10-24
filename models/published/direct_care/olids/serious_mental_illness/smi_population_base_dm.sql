{{
    config(
        materialized='view',
        tags=['smi_registry']
    )
}}

select * 
FROM {{ ref('int_smi_population_base') }}