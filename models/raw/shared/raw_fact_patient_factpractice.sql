-- Raw layer model for fact_patient.FactPractice
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_PatientID" as sk_patient_id,
    "SK_OrganisationID" as sk_organisation_id,
    "PeriodStart" as period_start,
    "PeriodEnd" as period_end,
    "DateDetectedJoin" as date_detected_join,
    "DateDetectedLeft" as date_detected_left
from {{ source('fact_patient', 'FactPractice') }}
