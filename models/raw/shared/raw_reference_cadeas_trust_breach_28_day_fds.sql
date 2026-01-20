{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__TRUST__BREACH__28_DAY_FDS \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__TRUST__BREACH__28_DAY_FDS'') \nColumns:\n  Provider.Code.FDS -> provider_code_fds\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  DCO -> dco\n  STP -> stp\n  CCG -> ccg\n  Provider.Code.DTT -> provider_code_dtt\n  Pathway.End.Reason -> pathway_end_reason\n  Priority.Type -> priority_type\n  Source.of.referral.for.outpatients -> source_of_referral_for_outpatients\n  Cancer.Report.Category -> cancer_report_category\n  Primary.Cancer.Site -> primary_cancer_site\n  Clock.Stop.Type -> clock_stop_type\n  Exclusion.Reason -> exclusion_reason\n  DelayReasonFDS -> delay_reason_fds\n  NoBreaching -> no_breaching\n  Trust_Name -> trust_name\n  Cancer_Alliance -> cancer_alliance\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "Provider.Code.FDS" as provider_code_fds,
    "Month.Number" as month_number,
    "Month" as month,
    "Year" as year,
    "DCO" as dco,
    "STP" as stp,
    "CCG" as ccg,
    "Provider.Code.DTT" as provider_code_dtt,
    "Pathway.End.Reason" as pathway_end_reason,
    "Priority.Type" as priority_type,
    "Source.of.referral.for.outpatients" as source_of_referral_for_outpatients,
    "Cancer.Report.Category" as cancer_report_category,
    "Primary.Cancer.Site" as primary_cancer_site,
    "Clock.Stop.Type" as clock_stop_type,
    "Exclusion.Reason" as exclusion_reason,
    "DelayReasonFDS" as delay_reason_fds,
    "NoBreaching" as no_breaching,
    "Trust_Name" as trust_name,
    "Cancer_Alliance" as cancer_alliance,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__TRUST__BREACH__28_DAY_FDS') }}
