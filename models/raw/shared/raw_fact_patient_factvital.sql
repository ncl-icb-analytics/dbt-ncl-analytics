-- Raw layer model for fact_patient.FactVital
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_PatientID" as sk_patient_id,
    "PeriodStart" as period_start,
    "PeriodEnd" as period_end,
    "BodyTemp" as body_temp,
    "RespiratoryRate" as respiratory_rate,
    "HeartRate" as heart_rate,
    "BloodPressureSystolic" as blood_pressure_systolic,
    "BloodPressureDiastolic" as blood_pressure_diastolic,
    "Height" as height,
    "Weight" as weight,
    "BMI" as bmi
from {{ source('fact_patient', 'FactVital') }}
