-- Raw layer model for fact_patient.MetaLifetimeConditionCodeMaskSnomed
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_LifetimeConditionTypeID" as sk_lifetime_condition_type_id,
    "Snomed" as snomed
from {{ source('fact_patient', 'MetaLifetimeConditionCodeMaskSnomed') }}
