-- Raw layer model for fact_patient.DimConditionType
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ConditionTypeID" as sk_condition_type_id,
    "ConditionType" as condition_type,
    "ConditionLifespan" as condition_lifespan,
    "Description" as description,
    "HasValue" as has_value,
    "SK_UnitID" as sk_unit_id
from {{ source('fact_patient', 'DimConditionType') }}
