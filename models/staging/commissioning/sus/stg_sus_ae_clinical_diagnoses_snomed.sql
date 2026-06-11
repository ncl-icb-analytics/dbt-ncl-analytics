{{
    config(materialized = 'view')
}}

select {{ dbt_utils.generate_surrogate_key(["primarykey_id", "snomed_id"]) }} as diagnosis_id
    ,primarykey_id
    , snomed_id
    , rownumber_id 
    , code
    , is_primary
from {{ ref('raw_sus_ae_clinical_diagnoses_snomed') }}
qualify row_number() over (partition by primarykey_id, snomed_id, code order by rownumber_id) = 1