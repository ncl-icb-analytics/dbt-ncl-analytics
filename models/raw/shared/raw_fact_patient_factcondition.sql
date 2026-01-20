{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.FactCondition \ndbt: source(''fact_patient'', ''FactCondition'') \nColumns:\n  SK_DataSourceID -> sk_data_source_id\n  SK_ConditionTypeID -> sk_condition_type_id\n  SK_PatientID -> sk_patient_id\n  PeriodStart -> period_start\n  PeriodEnd -> period_end\n  Value -> value\n  DateDetected -> date_detected"
    )
}}
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_ConditionTypeID" as sk_condition_type_id,
    "SK_PatientID" as sk_patient_id,
    "PeriodStart" as period_start,
    "PeriodEnd" as period_end,
    "Value" as value,
    "DateDetected" as date_detected
from {{ source('fact_patient', 'FactCondition') }}
