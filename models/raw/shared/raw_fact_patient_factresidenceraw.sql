-- Raw layer model for fact_patient.FactresidenceRaw
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_PatientID" as sk_patient_id,
    "SK_OutputAreaID" as sk_output_area_id,
    "SK_PostcodeID" as sk_postcode_id,
    "ResidenceStartDate" as residence_start_date,
    "ResidenceEndDate" as residence_end_date
from {{ source('fact_patient', 'FactresidenceRaw') }}
