{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__CCG__62_DAY_BREASTSYMP \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__CCG__62_DAY_BREASTSYMP'') \nColumns:\n  CCG -> ccg\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  DCO -> dco\n  STP -> stp\n  AccountableProvider.62.day.wait -> accountable_provider_62_day_wait\n  AccountableProvider.38.day.wait.investigative.phase -> accountable_provider_38_day_wait_investigative_phase\n  AccountableProvider.24.day.wait.treatment.phase -> accountable_provider_24_day_wait_treatment_phase\n  SeenProvider -> seen_provider\n  InvestigatingProvider1 -> investigating_provider1\n  InvestigatingProvider2 -> investigating_provider2\n  InvestigatingProvider3 -> investigating_provider3\n  InvestigatingProvider4 -> investigating_provider4\n  InvestigatingProvider5 -> investigating_provider5\n  InvestigatingProvider6 -> investigating_provider6\n  InvestigatingProvider7 -> investigating_provider7\n  InvestigatingProvider8 -> investigating_provider8\n  InvestigatingProvider9 -> investigating_provider9\n  InvestigatingProvider10 -> investigating_provider10\n  TreatmentProvider -> treatment_provider\n  Number.of.transfers -> number_of_transfers\n  Allocation.Rules -> allocation_rules\n  AdmittedCare -> admitted_care\n  TreatmentModality -> treatment_modality\n  CancerReportCategory -> cancer_report_category\n  NoPatients...AccountableProvider.62.day.wait -> no_patients_accountable_provider_62_day_wait\n  DaysWithin31...AccountableProvider.62.day.wait -> days_within31_accountable_provider_62_day_wait\n  Days32To62...AccountableProvider.62.day.wait -> days32_to62_accountable_provider_62_day_wait\n  Days63To76...AccountableProvider.62.day.wait -> days63_to76_accountable_provider_62_day_wait\n  Days77To90...AccountableProvider.62.day.wait -> days77_to90_accountable_provider_62_day_wait\n  Days91To104...AccountableProvider.62.day.wait -> days91_to104_accountable_provider_62_day_wait\n  DaysMoreThan104...AccountableProvider.62.day.wait -> days_more_than104_accountable_provider_62_day_wait\n  CCG18NM -> ccg18_nm\n  CALNCV18NM -> calncv18_nm\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "CCG" as ccg,
    "Month.Number" as month_number,
    "Month" as month,
    "Year" as year,
    "DCO" as dco,
    "STP" as stp,
    "AccountableProvider.62.day.wait" as accountable_provider_62_day_wait,
    "AccountableProvider.38.day.wait.investigative.phase" as accountable_provider_38_day_wait_investigative_phase,
    "AccountableProvider.24.day.wait.treatment.phase" as accountable_provider_24_day_wait_treatment_phase,
    "SeenProvider" as seen_provider,
    "InvestigatingProvider1" as investigating_provider1,
    "InvestigatingProvider2" as investigating_provider2,
    "InvestigatingProvider3" as investigating_provider3,
    "InvestigatingProvider4" as investigating_provider4,
    "InvestigatingProvider5" as investigating_provider5,
    "InvestigatingProvider6" as investigating_provider6,
    "InvestigatingProvider7" as investigating_provider7,
    "InvestigatingProvider8" as investigating_provider8,
    "InvestigatingProvider9" as investigating_provider9,
    "InvestigatingProvider10" as investigating_provider10,
    "TreatmentProvider" as treatment_provider,
    "Number.of.transfers" as number_of_transfers,
    "Allocation.Rules" as allocation_rules,
    "AdmittedCare" as admitted_care,
    "TreatmentModality" as treatment_modality,
    "CancerReportCategory" as cancer_report_category,
    "NoPatients...AccountableProvider.62.day.wait" as no_patients_accountable_provider_62_day_wait,
    "DaysWithin31...AccountableProvider.62.day.wait" as days_within31_accountable_provider_62_day_wait,
    "Days32To62...AccountableProvider.62.day.wait" as days32_to62_accountable_provider_62_day_wait,
    "Days63To76...AccountableProvider.62.day.wait" as days63_to76_accountable_provider_62_day_wait,
    "Days77To90...AccountableProvider.62.day.wait" as days77_to90_accountable_provider_62_day_wait,
    "Days91To104...AccountableProvider.62.day.wait" as days91_to104_accountable_provider_62_day_wait,
    "DaysMoreThan104...AccountableProvider.62.day.wait" as days_more_than104_accountable_provider_62_day_wait,
    "CCG18NM" as ccg18_nm,
    "CALNCV18NM" as calncv18_nm,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__CCG__62_DAY_BREASTSYMP') }}
