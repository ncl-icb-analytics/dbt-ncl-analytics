-- Raw layer model for fact_patient.MetaConditionCodeMaskReadMedication
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ConditionTypeID" as sk_condition_type_id,
    "CodeMask" as code_mask,
    "IsExclude" as is_exclude
from {{ source('fact_patient', 'MetaConditionCodeMaskReadMedication') }}
