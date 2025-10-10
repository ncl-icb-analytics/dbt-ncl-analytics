-- Raw layer model for reference_cancer_cwt_alliance.CADEAS__CCG__BREACH__2WW
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
    "FirstSeenDelayReason" as first_seen_delay_reason,
    "NoBreaching" as no_breaching,
    "STP" as stp,
    "CCG18NM" as ccg18_nm,
    "CALNCV18NM" as calncv18_nm,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__CCG__BREACH__2WW') }}
