{{
    config(materialized = 'view')
}}

select primarykey_id
    ,snomed_id
    ,rownumber_id
    ,code
    ,date
    ,time
from {{ ref('raw_sus_ecds_clinical_treatments_snomed') }}