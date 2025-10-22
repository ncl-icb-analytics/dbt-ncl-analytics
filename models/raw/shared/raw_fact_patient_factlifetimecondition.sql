-- Raw layer model for fact_patient.FactLifetimeCondition
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_LifetimeConditionTypeID" as sk_lifetime_condition_type_id,
    "SK_PatientID" as sk_patient_id,
    "DateFirstDetected" as date_first_detected
from {{ source('fact_patient', 'FactLifetimeCondition') }}
