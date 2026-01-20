{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__TRUST__BREACH__31_DAY \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__TRUST__BREACH__31_DAY'') \nColumns:\n  ProviderCode -> provider_code\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  TreatmentStage -> treatment_stage\n  DCO -> dco\n  CCG -> ccg\n  AdmittedCare -> admitted_care\n  TreatmentModality -> treatment_modality\n  CancerReportCategory -> cancer_report_category\n  DelayReasonDecisionToTreatment -> delay_reason_decision_to_treatment\n  NoBreaching -> no_breaching\n  STP -> stp\n  Trust_Name -> trust_name\n  Cancer_Alliance -> cancer_alliance\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "ProviderCode" as provider_code,
    "Month.Number" as month_number,
    "Month" as month,
    "Year" as year,
    "TreatmentStage" as treatment_stage,
    "DCO" as dco,
    "CCG" as ccg,
    "AdmittedCare" as admitted_care,
    "TreatmentModality" as treatment_modality,
    "CancerReportCategory" as cancer_report_category,
    "DelayReasonDecisionToTreatment" as delay_reason_decision_to_treatment,
    "NoBreaching" as no_breaching,
    "STP" as stp,
    "Trust_Name" as trust_name,
    "Cancer_Alliance" as cancer_alliance,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__TRUST__BREACH__31_DAY') }}
