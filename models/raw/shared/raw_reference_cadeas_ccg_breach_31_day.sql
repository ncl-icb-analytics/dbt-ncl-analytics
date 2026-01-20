{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__CCG__BREACH__31_DAY \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__CCG__BREACH__31_DAY'') \nColumns:\n  CCG -> ccg\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  TreatmentStage -> treatment_stage\n  DCO -> dco\n  ProviderCode -> provider_code\n  AdmittedCare -> admitted_care\n  TreatmentModality -> treatment_modality\n  CancerReportCategory -> cancer_report_category\n  DelayReasonDecisionToTreatment -> delay_reason_decision_to_treatment\n  NoBreaching -> no_breaching\n  STP -> stp\n  CCG18NM -> ccg18_nm\n  CALNCV18NM -> calncv18_nm\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "CCG" as ccg,
    "Month.Number" as month_number,
    "Month" as month,
    "Year" as year,
    "TreatmentStage" as treatment_stage,
    "DCO" as dco,
    "ProviderCode" as provider_code,
    "AdmittedCare" as admitted_care,
    "TreatmentModality" as treatment_modality,
    "CancerReportCategory" as cancer_report_category,
    "DelayReasonDecisionToTreatment" as delay_reason_decision_to_treatment,
    "NoBreaching" as no_breaching,
    "STP" as stp,
    "CCG18NM" as ccg18_nm,
    "CALNCV18NM" as calncv18_nm,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__CCG__BREACH__31_DAY') }}
