-- Raw layer model for fact_patient.DimRiskScore
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_RiskScoreID" as sk_risk_score_id,
    "RiskScore" as risk_score,
    "RiskScoreLifespan" as risk_score_lifespan,
    "Description" as description
from {{ source('fact_patient', 'DimRiskScore') }}
