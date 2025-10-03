-- Raw layer model for reference_cancer_cwt_alliance.CADEAS__CCG__2WW
-- Source: "DATA_LAKE__NCL"."CANCER__CWT_ALLIANCE"
-- Description: Cancer waiting times alliance data
-- This is a 1:1 passthrough from source with standardized column names
select
    "CCG" as ccg,
    "Month.Number" as month_number,
    "Month" as month,
    "Year" as year,
    "DCO" as dco,
    "ProviderCode" as provider_code,
    "CancerReportCategory" as cancer_report_category,
    "NoSeen" as no_seen,
    "DaysWithin14" as days_within14,
    "Days15to16" as days15to16,
    "Days17to21" as days17to21,
    "Days22to28" as days22to28,
    "DaysMoreThan28" as days_more_than28,
    "STP" as stp,
    "CCG18NM" as ccg18_nm,
    "CALNCV18NM" as calncv18_nm,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__CCG__2WW') }}
