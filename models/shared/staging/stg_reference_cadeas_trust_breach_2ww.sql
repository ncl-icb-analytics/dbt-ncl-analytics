-- Staging model for reference_cancer_cwt_alliance.CADEAS__TRUST__BREACH__2WW
-- Source: "DATA_LAKE__NCL"."CANCER__CWT_ALLIANCE"
-- Description: Cancer waiting times alliance data

select
    "ProviderCode" as provider_code,
    "Month.Number" as month_number,
    "Month" as month,
    "Year" as year,
    "DCO" as dco,
    "CCG" as ccg,
    "CancerReportCategory" as cancer_report_category,
    "FirstSeenDelayReason" as first_seen_delay_reason,
    "NoBreaching" as no_breaching,
    "STP" as stp,
    "Trust_Name" as trust_name,
    "Cancer_Alliance" as cancer_alliance,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__TRUST__BREACH__2WW') }}
