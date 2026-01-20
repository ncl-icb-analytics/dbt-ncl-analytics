{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.MetaLifetimeConditionCodeMaskRead \ndbt: source(''fact_patient'', ''MetaLifetimeConditionCodeMaskRead'') \nColumns:\n  SK_LifetimeConditionTypeID -> sk_lifetime_condition_type_id\n  CodeMask -> code_mask"
    )
}}
select
    "SK_LifetimeConditionTypeID" as sk_lifetime_condition_type_id,
    "CodeMask" as code_mask
from {{ source('fact_patient', 'MetaLifetimeConditionCodeMaskRead') }}
