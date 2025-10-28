-- Raw layer model for fact_patient.FactCareHomeRaw
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_PatientID" as sk_patient_id,
    "SK_OrganisationID" as sk_organisation_id,
    "Score" as score,
    "RegistrationStartDate" as registration_start_date,
    "RegistrationEndDate" as registration_end_date
from {{ source('fact_patient', 'FactCareHomeRaw') }}
