{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.MetaLifetimeConditionCodeMaskSnomed \ndbt: source(''fact_patient'', ''MetaLifetimeConditionCodeMaskSnomed'') \nColumns:\n  SK_LifetimeConditionTypeID -> sk_lifetime_condition_type_id\n  Snomed -> snomed"
    )
}}
select
    "SK_LifetimeConditionTypeID" as sk_lifetime_condition_type_id,
    "Snomed" as snomed
from {{ source('fact_patient', 'MetaLifetimeConditionCodeMaskSnomed') }}
