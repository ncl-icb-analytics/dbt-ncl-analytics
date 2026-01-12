{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.DimRiskScore \ndbt: source(''fact_patient'', ''DimRiskScore'') \nColumns:\n  SK_RiskScoreID -> sk_risk_score_id\n  RiskScore -> risk_score\n  RiskScoreLifespan -> risk_score_lifespan\n  Description -> description"
    )
}}
select
    "SK_RiskScoreID" as sk_risk_score_id,
    "RiskScore" as risk_score,
    "RiskScoreLifespan" as risk_score_lifespan,
    "Description" as description
from {{ source('fact_patient', 'DimRiskScore') }}
