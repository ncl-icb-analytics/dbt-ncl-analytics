-- Raw layer model for fact_patient.DimLifetimeConditionGroup
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_LifetimeConditionGroupID" as sk_lifetime_condition_group_id,
    "LifetimeConditionGroup" as lifetime_condition_group
from {{ source('fact_patient', 'DimLifetimeConditionGroup') }}
