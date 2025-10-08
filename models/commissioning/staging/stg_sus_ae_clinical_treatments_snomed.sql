{{
    config(materialized = 'view')
}}

select primarykey_id
    ,snomed_id
    ,rownumber_id
    ,code
from {{ ref('raw_sus_ae_clinical_treatments_snomed') }}