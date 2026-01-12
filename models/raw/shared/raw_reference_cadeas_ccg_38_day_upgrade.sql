{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__CCG__38_DAY_UPGRADE \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__CCG__38_DAY_UPGRADE'') \nColumns:\n  CCG -> ccg\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  DCO -> dco\n  STP -> stp\n  AccountableProvider.38.day.wait.investigative.phase -> accountable_provider_38_day_wait_investigative_phase\n  AdmittedCare -> admitted_care\n  TreatmentModality -> treatment_modality\n  CancerReportCategory -> cancer_report_category\n  NoPatients...AccountableProvider.38.day.wait -> no_patients_accountable_provider_38_day_wait\n  DaysWithin38...AccountableProvider.38.day.wait -> days_within38_accountable_provider_38_day_wait\n  DaysMoreThan38...AccountableProvider.38.day.wait -> days_more_than38_accountable_provider_38_day_wait\n  CCG18NM -> ccg18_nm\n  CALNCV18NM -> calncv18_nm\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "CCG" as ccg,
    "Month.Number" as month_number,
    "Month" as month,
    "Year" as year,
    "DCO" as dco,
    "STP" as stp,
    "AccountableProvider.38.day.wait.investigative.phase" as accountable_provider_38_day_wait_investigative_phase,
    "AdmittedCare" as admitted_care,
    "TreatmentModality" as treatment_modality,
    "CancerReportCategory" as cancer_report_category,
    "NoPatients...AccountableProvider.38.day.wait" as no_patients_accountable_provider_38_day_wait,
    "DaysWithin38...AccountableProvider.38.day.wait" as days_within38_accountable_provider_38_day_wait,
    "DaysMoreThan38...AccountableProvider.38.day.wait" as days_more_than38_accountable_provider_38_day_wait,
    "CCG18NM" as ccg18_nm,
    "CALNCV18NM" as calncv18_nm,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__CCG__38_DAY_UPGRADE') }}
