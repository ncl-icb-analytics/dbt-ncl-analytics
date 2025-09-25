{{
    config(
        materialized='view',
        tags=['childhood_imms']
    )
}}

select * 
FROM {{ ref('int_childhood_imms_person_level_adolescent') }}