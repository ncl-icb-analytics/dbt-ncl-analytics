{{
    config(
        description="Raw layer (Cancer waiting times alliance data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__CWT_ALLIANCE.CADEAS__TRUST__2WW \ndbt: source(''reference_cancer_cwt_alliance'', ''CADEAS__TRUST__2WW'') \nColumns:\n  ProviderCode -> provider_code\n  Month.Number -> month_number\n  Month -> month\n  Year -> year\n  DCO -> dco\n  CCG -> ccg\n  CancerReportCategory -> cancer_report_category\n  NoSeen -> no_seen\n  DaysWithin14 -> days_within14\n  Days15to16 -> days15to16\n  Days17to21 -> days17to21\n  Days22to28 -> days22to28\n  DaysMoreThan28 -> days_more_than28\n  STP -> stp\n  Trust_Name -> trust_name\n  Cancer_Alliance -> cancer_alliance\n  _TIMESTAMP -> timestamp"
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
    "NoSeen" as no_seen,
    "DaysWithin14" as days_within14,
    "Days15to16" as days15to16,
    "Days17to21" as days17to21,
    "Days22to28" as days22to28,
    "DaysMoreThan28" as days_more_than28,
    "STP" as stp,
    "Trust_Name" as trust_name,
    "Cancer_Alliance" as cancer_alliance,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_cwt_alliance', 'CADEAS__TRUST__2WW') }}
