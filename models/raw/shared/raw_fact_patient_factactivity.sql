{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.FactActivity \ndbt: source(''fact_patient'', ''FactActivity'') \nColumns:\n  SK_DataSourceID -> sk_data_source_id\n  SK_PatientID -> sk_patient_id\n  Period -> period\n  ActivityCount -> activity_count\n  TotalCost -> total_cost\n  BedDays -> bed_days\n  NoTreatment -> no_treatment\n  AEAdmission -> ae_admission"
    )
}}
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
