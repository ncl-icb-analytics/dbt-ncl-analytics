-- Staging model for reference_cancer_cwt_alliance.CADEAS__CCG__31_DAY
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
    "CCG18NM" as ccg18_nm,
    "CALNCV18NM" as calncv18_nm,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__CCG__31_DAY') }}
