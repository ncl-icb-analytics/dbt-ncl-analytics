{{
    config(materialized = 'view')
}}

select primarykey_id
        ,icd_id 
        ,rownumber_id 
        ,episodes_id
        ,code
from {{ ref('raw_sus_apc_spell_episodes_clinical_coding_diagnosis_icd') }}