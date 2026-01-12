{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__CCG__BREACH__28_DAY_FDS \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__CCG__BREACH__28_DAY_FDS'') \nColumns:\n  CCG -> ccg\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  DCO -> dco\n  STP -> stp\n  Provider.Code.FDS -> provider_code_fds\n  Provider.Code.DTT -> provider_code_dtt\n  Pathway.End.Reason -> pathway_end_reason\n  Priority.Type -> priority_type\n  Source.of.referral.for.outpatients -> source_of_referral_for_outpatients\n  Cancer.Report.Category -> cancer_report_category\n  Primary.Cancer.Site -> primary_cancer_site\n  Clock.Stop.Type -> clock_stop_type\n  Exclusion.Reason -> exclusion_reason\n  DelayReasonFDS -> delay_reason_fds\n  NoBreaching -> no_breaching\n  CCG18NM -> ccg18_nm\n  CALNCV18NM -> calncv18_nm\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "CCG" as ccg,
    "Month.Number" as month_number,
    "Month" as month,
    "Year" as year,
    "DCO" as dco,
    "STP" as stp,
    "Provider.Code.FDS" as provider_code_fds,
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
    "CCG18NM" as ccg18_nm,
    "CALNCV18NM" as calncv18_nm,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__CCG__BREACH__28_DAY_FDS') }}
