{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.FactPractice \ndbt: source(''fact_patient'', ''FactPractice'') \nColumns:\n  SK_DataSourceID -> sk_data_source_id\n  SK_PatientID -> sk_patient_id\n  SK_OrganisationID -> sk_organisation_id\n  PeriodStart -> period_start\n  PeriodEnd -> period_end\n  DateDetectedJoin -> date_detected_join\n  DateDetectedLeft -> date_detected_left"
    )
}}
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_PatientID" as sk_patient_id,
    "SK_OrganisationID" as sk_organisation_id,
    "PeriodStart" as period_start,
    "PeriodEnd" as period_end,
    "DateDetectedJoin" as date_detected_join,
    "DateDetectedLeft" as date_detected_left
from {{ source('fact_patient', 'FactPractice') }}
