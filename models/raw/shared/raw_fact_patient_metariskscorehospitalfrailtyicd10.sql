{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.MetaRiskScoreHospitalFrailtyICD10 \ndbt: source(''fact_patient'', ''MetaRiskScoreHospitalFrailtyICD10'') \nColumns:\n  SK_HospitalFrailtyID -> sk_hospital_frailty_id\n  Code -> code\n  Score -> score"
    )
}}
select
    "SK_HospitalFrailtyID" as sk_hospital_frailty_id,
    "Code" as code,
    "Score" as score
from {{ source('fact_patient', 'MetaRiskScoreHospitalFrailtyICD10') }}
