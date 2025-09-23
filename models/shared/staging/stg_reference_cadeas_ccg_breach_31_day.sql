-- Staging model for reference_cancer_cwt_alliance.CADEAS__CCG__BREACH__31_DAY
-- Source: "DATA_LAKE__NCL"."CANCER__CWT_ALLIANCE"
-- Description: Cancer waiting times alliance data

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
