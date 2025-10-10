-- Raw layer model for reference_cancer_cwt_alliance.CADEAS__TRUST__BREACH__31_DAY
-- Source: "DATA_LAKE__NCL"."CANCER__CWT_ALLIANCE"
-- Description: Cancer waiting times alliance data
-- This is a 1:1 passthrough from source with standardized column names
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
