{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.MetaVitalRead \ndbt: source(''fact_patient'', ''MetaVitalRead'') \nColumns:\n  SK_VitalID -> sk_vital_id\n  CodeMask -> code_mask"
    )
}}
select
    "SK_VitalID" as sk_vital_id,
    "CodeMask" as code_mask
from {{ source('fact_patient', 'MetaVitalRead') }}
