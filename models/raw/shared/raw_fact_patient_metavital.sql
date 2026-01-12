{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.MetaVital \ndbt: source(''fact_patient'', ''MetaVital'') \nColumns:\n  SK_VitalID -> sk_vital_id\n  SK_UnitID -> sk_unit_id\n  Vital -> vital"
    )
}}
select
    "SK_VitalID" as sk_vital_id,
    "SK_UnitID" as sk_unit_id,
    "Vital" as vital
from {{ source('fact_patient', 'MetaVital') }}
