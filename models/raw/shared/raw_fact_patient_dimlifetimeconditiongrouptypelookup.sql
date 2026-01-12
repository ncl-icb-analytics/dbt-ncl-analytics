{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.DimLifetimeConditionGroupTypeLookup \ndbt: source(''fact_patient'', ''DimLifetimeConditionGroupTypeLookup'') \nColumns:\n  SK_LifetimeConditionGroupID -> sk_lifetime_condition_group_id\n  SK_LifetimeConditionTypeID -> sk_lifetime_condition_type_id\n  Weighting -> weighting"
    )
}}
select
    "SK_LifetimeConditionGroupID" as sk_lifetime_condition_group_id,
    "SK_LifetimeConditionTypeID" as sk_lifetime_condition_type_id,
    "Weighting" as weighting
from {{ source('fact_patient', 'DimLifetimeConditionGroupTypeLookup') }}
