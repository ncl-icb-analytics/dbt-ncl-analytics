-- Raw layer model for fact_patient.MetaVitalSnomed
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_VitalID" as sk_vital_id,
    "Snomed" as snomed
from {{ source('fact_patient', 'MetaVitalSnomed') }}
