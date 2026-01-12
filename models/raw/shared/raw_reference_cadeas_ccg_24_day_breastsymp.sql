{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__CCG__24_DAY_BREASTSYMP \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__CCG__24_DAY_BREASTSYMP'') \nColumns:\n  CCG -> ccg\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  DCO -> dco\n  STP -> stp\n  AccountableProvider.24.day.wait.treatment.phase -> accountable_provider_24_day_wait_treatment_phase\n  AdmittedCare -> admitted_care\n  TreatmentModality -> treatment_modality\n  CancerReportCategory -> cancer_report_category\n  NoPatients...AccountableProvider.24.day.wait -> no_patients_accountable_provider_24_day_wait\n  DaysWithin24...AccountableProvider.24.day.wait -> days_within24_accountable_provider_24_day_wait\n  Days25To31...AccountableProvider.24.day.wait -> days25_to31_accountable_provider_24_day_wait\n  DaysMoreThan31...AccountableProvider.24.day.wait -> days_more_than31_accountable_provider_24_day_wait\n  CCG18NM -> ccg18_nm\n  CALNCV18NM -> calncv18_nm\n  _TIMESTAMP -> timestamp"
    )
}}
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
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__CCG__24_DAY_BREASTSYMP') }}
