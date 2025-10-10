-- Raw layer model for reference_cancer_cwt_alliance.CADEAS__CCG__28_DAY_FDS
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
    "Provider.Code.FDS" as provider_code_fds,
    "Provider.Code.DTT" as provider_code_dtt,
    "Pathway.End.Reason" as pathway_end_reason,
    "Priority.Type" as priority_type,
    "Source.of.referral.for.outpatients" as source_of_referral_for_outpatients,
    "Cancer.Report.Category" as cancer_report_category,
    "Primary.Cancer.Site" as primary_cancer_site,
    "Clock.Stop.Type" as clock_stop_type,
    "Exclusion.Reason" as exclusion_reason,
    "NoPatients" as no_patients,
    "DaysWithin7" as days_within7,
    "Days8To14" as days8_to14,
    "Days15To21" as days15_to21,
    "Days22To28" as days22_to28,
    "Days29To35" as days29_to35,
    "Days36To42" as days36_to42,
    "Days43To49" as days43_to49,
    "Days50To62" as days50_to62,
    "Days63To76" as days63_to76,
    "Days77To90" as days77_to90,
    "Days91To104" as days91_to104,
    "DaysMoreThan104" as days_more_than104,
    "CCG18NM" as ccg18_nm,
    "CALNCV18NM" as calncv18_nm,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__CCG__28_DAY_FDS') }}
