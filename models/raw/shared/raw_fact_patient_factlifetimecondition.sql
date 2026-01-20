{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.FactLifetimeCondition \ndbt: source(''fact_patient'', ''FactLifetimeCondition'') \nColumns:\n  SK_DataSourceID -> sk_data_source_id\n  SK_LifetimeConditionTypeID -> sk_lifetime_condition_type_id\n  SK_PatientID -> sk_patient_id\n  DateFirstDetected -> date_first_detected"
    )
}}
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_LifetimeConditionTypeID" as sk_lifetime_condition_type_id,
    "SK_PatientID" as sk_patient_id,
    "DateFirstDetected" as date_first_detected
from {{ source('fact_patient', 'FactLifetimeCondition') }}
