{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.FactRiskScore \ndbt: source(''fact_patient'', ''FactRiskScore'') \nColumns:\n  SK_RiskScoreID -> sk_risk_score_id\n  SK_PatientID -> sk_patient_id\n  PeriodStart -> period_start\n  PeriodEnd -> period_end\n  SK_RiskScoreBandID -> sk_risk_score_band_id\n  Value -> value\n  DateDetected -> date_detected"
    )
}}
select
    "SK_RiskScoreID" as sk_risk_score_id,
    "SK_PatientID" as sk_patient_id,
    "PeriodStart" as period_start,
    "PeriodEnd" as period_end,
    "SK_RiskScoreBandID" as sk_risk_score_band_id,
    "Value" as value,
    "DateDetected" as date_detected
from {{ source('fact_patient', 'FactRiskScore') }}
