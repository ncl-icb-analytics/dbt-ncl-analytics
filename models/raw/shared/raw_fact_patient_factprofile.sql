-- Raw layer model for fact_patient.FactProfile
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_PatientID" as sk_patient_id,
    "PeriodStart" as period_start,
    "PeriodEnd" as period_end,
    "DateOfBirth" as date_of_birth,
    "SK_GenderID" as sk_gender_id,
    "SK_EthnicityID" as sk_ethnicity_id,
    "DateOfDeath" as date_of_death
from {{ source('fact_patient', 'FactProfile') }}
