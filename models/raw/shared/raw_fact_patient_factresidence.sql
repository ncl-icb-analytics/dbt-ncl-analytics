{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.Factresidence \ndbt: source(''fact_patient'', ''Factresidence'') \nColumns:\n  SK_DataSourceID -> sk_data_source_id\n  SK_PatientID -> sk_patient_id\n  SK_OutputAreaID -> sk_output_area_id\n  SK_PostcodeID -> sk_postcode_id\n  PeriodStart -> period_start\n  PeriodEnd -> period_end\n  DateDetectedStart -> date_detected_start\n  DateDetectedEnd -> date_detected_end"
    )
}}
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
