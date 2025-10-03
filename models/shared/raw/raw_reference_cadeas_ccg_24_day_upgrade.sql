-- Raw layer model for reference_cancer_cwt_alliance.CADEAS__CCG__24_DAY_UPGRADE
-- Source: "DATA_LAKE__NCL"."CANCER__CWT_ALLIANCE"
-- Description: Cancer waiting times alliance data
-- This is a 1:1 passthrough from source with standardized column names
select
    "CCG" as ccg,
    "Month.Number" as month_number,
    "Month" as month,
    "Year" as year,
    "DCO" as dco,
    "STP" as stp,
    "AccountableProvider.24.day.wait.treatment.phase" as accountable_provider_24_day_wait_treatment_phase,
    "AdmittedCare" as admitted_care,
    "TreatmentModality" as treatment_modality,
    "CancerReportCategory" as cancer_report_category,
    "NoPatients...AccountableProvider.24.day.wait" as no_patients_accountable_provider_24_day_wait,
    "DaysWithin24...AccountableProvider.24.day.wait" as days_within24_accountable_provider_24_day_wait,
    "Days25To31...AccountableProvider.24.day.wait" as days25_to31_accountable_provider_24_day_wait,
    "DaysMoreThan31...AccountableProvider.24.day.wait" as days_more_than31_accountable_provider_24_day_wait,
    "CCG18NM" as ccg18_nm,
    "CALNCV18NM" as calncv18_nm,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__CCG__24_DAY_UPGRADE') }}
