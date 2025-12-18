{{
    config(materialized = 'view')
}}

select primarykey_id
    , snomed_id
    , rownumber_id 
    , code
    , is_primary
from {{ ref('raw_sus_ae_clinical_diagnoses_snomed') }}