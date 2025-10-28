-- Raw layer model for fact_patient.DimLifetimeConditionGroupTypeLookup
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_LifetimeConditionGroupID" as sk_lifetime_condition_group_id,
    "SK_LifetimeConditionTypeID" as sk_lifetime_condition_type_id,
    "Weighting" as weighting
from {{ source('fact_patient', 'DimLifetimeConditionGroupTypeLookup') }}
