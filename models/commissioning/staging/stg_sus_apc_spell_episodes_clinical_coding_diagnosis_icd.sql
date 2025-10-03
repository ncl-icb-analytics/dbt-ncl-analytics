{{
    config(materialized = 'view')
}}

select {{ dbt_utils.generate_surrogate_key(["primarykey_id", "episodes_id", "icd_id"]) }} as diagnosis_id
    ,primarykey_id
        ,icd_id 
        ,rownumber_id 
        ,episodes_id
        ,code
from {{ ref('raw_sus_apc_spell_episodes_clinical_coding_diagnosis_icd') }}