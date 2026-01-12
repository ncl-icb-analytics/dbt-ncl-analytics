{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.DimConditionType \ndbt: source(''fact_patient'', ''DimConditionType'') \nColumns:\n  SK_ConditionTypeID -> sk_condition_type_id\n  ConditionType -> condition_type\n  ConditionLifespan -> condition_lifespan\n  Description -> description\n  HasValue -> has_value\n  SK_UnitID -> sk_unit_id"
    )
}}
select
    "SK_ConditionTypeID" as sk_condition_type_id,
    "ConditionType" as condition_type,
    "ConditionLifespan" as condition_lifespan,
    "Description" as description,
    "HasValue" as has_value,
    "SK_UnitID" as sk_unit_id
from {{ source('fact_patient', 'DimConditionType') }}
