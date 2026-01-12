{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.DimRiskScoreBand \ndbt: source(''fact_patient'', ''DimRiskScoreBand'') \nColumns:\n  SK_RiskScoreBandID -> sk_risk_score_band_id\n  SK_RiskScoreID -> sk_risk_score_id\n  RiskScoreBand -> risk_score_band\n  LowerBound -> lower_bound\n  UpperBound -> upper_bound"
    )
}}
select
    "SK_RiskScoreBandID" as sk_risk_score_band_id,
    "SK_RiskScoreID" as sk_risk_score_id,
    "RiskScoreBand" as risk_score_band,
    "LowerBound" as lower_bound,
    "UpperBound" as upper_bound
from {{ source('fact_patient', 'DimRiskScoreBand') }}
