{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.MetaLifetimeConditionCodeMaskICD10 \ndbt: source(''fact_patient'', ''MetaLifetimeConditionCodeMaskICD10'') \nColumns:\n  SK_LifetimeConditionTypeID -> sk_lifetime_condition_type_id\n  CodeMask -> code_mask"
    )
}}
select
    "SK_LifetimeConditionTypeID" as sk_lifetime_condition_type_id,
    "CodeMask" as code_mask
from {{ source('fact_patient', 'MetaLifetimeConditionCodeMaskICD10') }}
