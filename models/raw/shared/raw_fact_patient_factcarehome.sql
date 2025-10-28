-- Raw layer model for fact_patient.FactCareHome
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_PatientID" as sk_patient_id,
    "PeriodStart" as period_start,
    "PeriodEnd" as period_end,
    "SK_OrganisationID" as sk_organisation_id,
    "Score" as score,
    "DateDetectedJoin" as date_detected_join,
    "DateDetectedLeft" as date_detected_left
from {{ source('fact_patient', 'FactCareHome') }}
