{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.DimLifetimeConditionType \ndbt: source(''fact_patient'', ''DimLifetimeConditionType'') \nColumns:\n  SK_LifetimeConditionTypeID -> sk_lifetime_condition_type_id\n  LifetimeConditionType -> lifetime_condition_type\n  Description -> description"
    )
}}
select
    "SK_LifetimeConditionTypeID" as sk_lifetime_condition_type_id,
    "LifetimeConditionType" as lifetime_condition_type,
    "Description" as description
from {{ source('fact_patient', 'DimLifetimeConditionType') }}
