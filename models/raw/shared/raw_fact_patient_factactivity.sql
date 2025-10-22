-- Raw layer model for fact_patient.FactActivity
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_PatientID" as sk_patient_id,
    "Period" as period,
    "ActivityCount" as activity_count,
    "TotalCost" as total_cost,
    "BedDays" as bed_days,
    "NoTreatment" as no_treatment,
    "AEAdmission" as ae_admission
from {{ source('fact_patient', 'FactActivity') }}
