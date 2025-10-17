-- Raw layer model for fact_patient.FactRiskScore
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_RiskScoreID" as sk_risk_score_id,
    "SK_PatientID" as sk_patient_id,
    "PeriodStart" as period_start,
    "PeriodEnd" as period_end,
    "SK_RiskScoreBandID" as sk_risk_score_band_id,
    "Value" as value,
    "DateDetected" as date_detected
from {{ source('fact_patient', 'FactRiskScore') }}
