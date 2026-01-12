{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__CCG__BREACH__2WW \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__CCG__BREACH__2WW'') \nColumns:\n  CCG -> ccg\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  DCO -> dco\n  ProviderCode -> provider_code\n  CancerReportCategory -> cancer_report_category\n  FirstSeenDelayReason -> first_seen_delay_reason\n  NoBreaching -> no_breaching\n  STP -> stp\n  CCG18NM -> ccg18_nm\n  CALNCV18NM -> calncv18_nm\n  _TIMESTAMP -> timestamp"
    )
}}
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
