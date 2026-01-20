{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__TRUST__BREACH__62_DAY_CLASSIC \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__TRUST__BREACH__62_DAY_CLASSIC'') \nColumns:\n  AccountableProvider.62.day.wait -> accountable_provider_62_day_wait\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  DCO -> dco\n  STP -> stp\n  CCG -> ccg\n  AccountableProvider.38.day.wait.investigative.phase -> accountable_provider_38_day_wait_investigative_phase\n  AccountableProvider.24.day.wait.treatment.phase -> accountable_provider_24_day_wait_treatment_phase\n  SeenProvider -> seen_provider\n  InvestigatingProvider1 -> investigating_provider1\n  InvestigatingProvider2 -> investigating_provider2\n  InvestigatingProvider3 -> investigating_provider3\n  InvestigatingProvider4 -> investigating_provider4\n  InvestigatingProvider5 -> investigating_provider5\n  InvestigatingProvider6 -> investigating_provider6\n  InvestigatingProvider7 -> investigating_provider7\n  InvestigatingProvider8 -> investigating_provider8\n  InvestigatingProvider9 -> investigating_provider9\n  InvestigatingProvider10 -> investigating_provider10\n  TreatmentProvider -> treatment_provider\n  Number.of.Transfers -> number_of_transfers\n  Allocation.Rules -> allocation_rules\n  AdmittedCare -> admitted_care\n  TreatmentModality -> treatment_modality\n  StandardBreached -> standard_breached\n  CancerReportCategory -> cancer_report_category\n  DelayReasonReferralToTreatment -> delay_reason_referral_to_treatment\n  NoBreaching -> no_breaching\n  Trust_Name -> trust_name\n  Cancer_Alliance -> cancer_alliance\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "AccountableProvider.62.day.wait" as accountable_provider_62_day_wait,
    "Month.Number" as month_number,
    "Month" as month,
    "Year" as year,
    "DCO" as dco,
    "STP" as stp,
    "CCG" as ccg,
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
    "Number.of.Transfers" as number_of_transfers,
    "Allocation.Rules" as allocation_rules,
    "AdmittedCare" as admitted_care,
    "TreatmentModality" as treatment_modality,
    "StandardBreached" as standard_breached,
    "CancerReportCategory" as cancer_report_category,
    "DelayReasonReferralToTreatment" as delay_reason_referral_to_treatment,
    "NoBreaching" as no_breaching,
    "Trust_Name" as trust_name,
    "Cancer_Alliance" as cancer_alliance,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__TRUST__BREACH__62_DAY_CLASSIC') }}
