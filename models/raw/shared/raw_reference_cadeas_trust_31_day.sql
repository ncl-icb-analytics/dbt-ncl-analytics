{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__TRUST__31_DAY \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__TRUST__31_DAY'') \nColumns:\n  ProviderCode -> provider_code\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  TreatmentStage -> treatment_stage\n  DCO -> dco\n  CCG -> ccg\n  AdmittedCare -> admitted_care\n  TreatmentModality -> treatment_modality\n  CancerReportCategory -> cancer_report_category\n  No2WWCancer -> no2_ww_cancer\n  No2WWBreastSymptoms -> no2_ww_breast_symptoms\n  NoUrgentScreening -> no_urgent_screening\n  NoOther -> no_other\n  NoTreated -> no_treated\n  DaysWithin31 -> days_within31\n  Days32to38 -> days32to38\n  Days39to48 -> days39to48\n  Days49to62 -> days49to62\n  DaysMoreThan62 -> days_more_than62\n  STP -> stp\n  Trust_Name -> trust_name\n  Cancer_Alliance -> cancer_alliance\n  _TIMESTAMP -> timestamp"
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
    "No2WWCancer" as no2_ww_cancer,
    "No2WWBreastSymptoms" as no2_ww_breast_symptoms,
    "NoUrgentScreening" as no_urgent_screening,
    "NoOther" as no_other,
    "NoTreated" as no_treated,
    "DaysWithin31" as days_within31,
    "Days32to38" as days32to38,
    "Days39to48" as days39to48,
    "Days49to62" as days49to62,
    "DaysMoreThan62" as days_more_than62,
    "STP" as stp,
    "Trust_Name" as trust_name,
    "Cancer_Alliance" as cancer_alliance,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__TRUST__31_DAY') }}
