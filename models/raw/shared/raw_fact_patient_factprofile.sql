{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.FactProfile \ndbt: source(''fact_patient'', ''FactProfile'') \nColumns:\n  SK_DataSourceID -> sk_data_source_id\n  SK_PatientID -> sk_patient_id\n  PeriodStart -> period_start\n  PeriodEnd -> period_end\n  DateOfBirth -> date_of_birth\n  SK_GenderID -> sk_gender_id\n  SK_EthnicityID -> sk_ethnicity_id\n  DateOfDeath -> date_of_death"
    )
}}
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
