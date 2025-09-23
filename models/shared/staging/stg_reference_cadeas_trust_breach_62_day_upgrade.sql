-- Staging model for reference_cancer_cwt_alliance.CADEAS__TRUST__BREACH__62_DAY_UPGRADE
-- Source: "DATA_LAKE__NCL"."CANCER__CWT_ALLIANCE"
-- Description: Cancer waiting times alliance data

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
    "CancerReportCategory" as cancer_report_category,
    "DelayReasonReferralToTreatment" as delay_reason_referral_to_treatment,
    "NoBreaching" as no_breaching,
    "Trust_Name" as trust_name,
    "Cancer_Alliance" as cancer_alliance,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__TRUST__BREACH__62_DAY_UPGRADE') }}
