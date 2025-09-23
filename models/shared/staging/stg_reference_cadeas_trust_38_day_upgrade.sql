-- Staging model for reference_cancer_cwt_alliance.CADEAS__TRUST__38_DAY_UPGRADE
-- Source: "DATA_LAKE__NCL"."CANCER__CWT_ALLIANCE"
-- Description: Cancer waiting times alliance data

select
    "AccountableProvider.38.day.wait.investigative.phase" as accountable_provider_38_day_wait_investigative_phase,
    "Month.Number" as month_number,
    "Month" as month,
    "Year" as year,
    "DCO" as dco,
    "CCG" as ccg,
    "STP" as stp,
    "AdmittedCare" as admitted_care,
    "TreatmentModality" as treatment_modality,
    "CancerReportCategory" as cancer_report_category,
    "NoPatients...AccountableProvider.38.day.wait" as no_patients_accountable_provider_38_day_wait,
    "DaysWithin38...AccountableProvider.38.day.wait" as days_within38_accountable_provider_38_day_wait,
    "DaysMoreThan38...AccountableProvider.38.day.wait" as days_more_than38_accountable_provider_38_day_wait,
    "Trust_Name" as trust_name,
    "Cancer_Alliance" as cancer_alliance,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__TRUST__38_DAY_UPGRADE') }}
