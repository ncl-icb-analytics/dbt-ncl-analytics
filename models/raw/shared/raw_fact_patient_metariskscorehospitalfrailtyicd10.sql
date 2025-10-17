-- Raw layer model for fact_patient.MetaRiskScoreHospitalFrailtyICD10
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_HospitalFrailtyID" as sk_hospital_frailty_id,
    "Code" as code,
    "Score" as score
from {{ source('fact_patient', 'MetaRiskScoreHospitalFrailtyICD10') }}
