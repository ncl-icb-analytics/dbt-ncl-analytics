{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.FactVital \ndbt: source(''fact_patient'', ''FactVital'') \nColumns:\n  SK_DataSourceID -> sk_data_source_id\n  SK_PatientID -> sk_patient_id\n  PeriodStart -> period_start\n  PeriodEnd -> period_end\n  BodyTemp -> body_temp\n  RespiratoryRate -> respiratory_rate\n  HeartRate -> heart_rate\n  BloodPressureSystolic -> blood_pressure_systolic\n  BloodPressureDiastolic -> blood_pressure_diastolic\n  Height -> height\n  Weight -> weight\n  BMI -> bmi"
    )
}}
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
