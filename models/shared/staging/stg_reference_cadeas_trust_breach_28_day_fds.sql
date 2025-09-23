-- Staging model for reference_cancer_cwt_alliance.CADEAS__TRUST__BREACH__28_DAY_FDS
-- Source: "DATA_LAKE__NCL"."CANCER__CWT_ALLIANCE"
-- Description: Cancer waiting times alliance data

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
