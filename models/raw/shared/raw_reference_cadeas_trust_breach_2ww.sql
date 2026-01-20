{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__TRUST__BREACH__2WW \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__TRUST__BREACH__2WW'') \nColumns:\n  ProviderCode -> provider_code\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  DCO -> dco\n  CCG -> ccg\n  CancerReportCategory -> cancer_report_category\n  FirstSeenDelayReason -> first_seen_delay_reason\n  NoBreaching -> no_breaching\n  STP -> stp\n  Trust_Name -> trust_name\n  Cancer_Alliance -> cancer_alliance\n  _TIMESTAMP -> timestamp"
    )
}}
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
