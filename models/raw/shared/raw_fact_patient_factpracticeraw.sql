{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.FactPracticeRaw \ndbt: source(''fact_patient'', ''FactPracticeRaw'') \nColumns:\n  SK_DataSourceID -> sk_data_source_id\n  SK_PatientID -> sk_patient_id\n  SK_OrganisationID -> sk_organisation_id\n  RegistrationStartDate -> registration_start_date\n  RegistrationEndDate -> registration_end_date"
    )
}}
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_PatientID" as sk_patient_id,
    "SK_OrganisationID" as sk_organisation_id,
    "RegistrationStartDate" as registration_start_date,
    "RegistrationEndDate" as registration_end_date
from {{ source('fact_patient', 'FactPracticeRaw') }}
