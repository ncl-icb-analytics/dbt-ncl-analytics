{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.MetaVitalSnomed \ndbt: source(''fact_patient'', ''MetaVitalSnomed'') \nColumns:\n  SK_VitalID -> sk_vital_id\n  Snomed -> snomed"
    )
}}
select
    "SK_VitalID" as sk_vital_id,
    "Snomed" as snomed
from {{ source('fact_patient', 'MetaVitalSnomed') }}
