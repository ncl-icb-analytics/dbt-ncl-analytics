{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.MetaConditionCodeMaskRead \ndbt: source(''fact_patient'', ''MetaConditionCodeMaskRead'') \nColumns:\n  SK_ConditionTypeID -> sk_condition_type_id\n  CodeMask -> code_mask\n  IsExclude -> is_exclude"
    )
}}
select
    "SK_ConditionTypeID" as sk_condition_type_id,
    "CodeMask" as code_mask,
    "IsExclude" as is_exclude
from {{ source('fact_patient', 'MetaConditionCodeMaskRead') }}
