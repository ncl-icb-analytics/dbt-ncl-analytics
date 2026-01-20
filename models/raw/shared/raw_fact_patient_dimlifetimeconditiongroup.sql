{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.DimLifetimeConditionGroup \ndbt: source(''fact_patient'', ''DimLifetimeConditionGroup'') \nColumns:\n  SK_LifetimeConditionGroupID -> sk_lifetime_condition_group_id\n  LifetimeConditionGroup -> lifetime_condition_group"
    )
}}
select
    "SK_LifetimeConditionGroupID" as sk_lifetime_condition_group_id,
    "LifetimeConditionGroup" as lifetime_condition_group
from {{ source('fact_patient', 'DimLifetimeConditionGroup') }}
