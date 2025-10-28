-- Raw layer model for fact_patient.DimRiskScoreBand
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_RiskScoreBandID" as sk_risk_score_band_id,
    "SK_RiskScoreID" as sk_risk_score_id,
    "RiskScoreBand" as risk_score_band,
    "LowerBound" as lower_bound,
    "UpperBound" as upper_bound
from {{ source('fact_patient', 'DimRiskScoreBand') }}
