{{
    config(materialized = 'view')
}}

select primarykey_id
            ,icd_id 
            ,rownumber_id 
            ,code
            ,present_on_admission
from {{ ref('raw_sus_op_appointment_clinical_coding_diagnosis_icd') }}