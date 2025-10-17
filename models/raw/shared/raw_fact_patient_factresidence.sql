-- Raw layer model for fact_patient.Factresidence
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_PatientID" as sk_patient_id,
    "SK_OutputAreaID" as sk_output_area_id,
    "SK_PostcodeID" as sk_postcode_id,
    "PeriodStart" as period_start,
    "PeriodEnd" as period_end,
    "DateDetectedStart" as date_detected_start,
    "DateDetectedEnd" as date_detected_end
from {{ source('fact_patient', 'Factresidence') }}
