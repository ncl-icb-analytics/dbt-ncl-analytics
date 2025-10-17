-- Raw layer model for fact_patient.FactCondition
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_ConditionTypeID" as sk_condition_type_id,
    "SK_PatientID" as sk_patient_id,
    "PeriodStart" as period_start,
    "PeriodEnd" as period_end,
    "Value" as value,
    "DateDetected" as date_detected
from {{ source('fact_patient', 'FactCondition') }}
