{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.RTTPeriodStatus \ndbt: source(''dictionary_dbo'', ''RTTPeriodStatus'') \nColumns:\n  SK_RTTPeriodStatusID -> sk_rtt_period_status_id\n  BK_RTTPeriodStatusCode -> bk_rtt_period_status_code\n  RTTPeriodStatusCategory -> rtt_period_status_category\n  RTTPeriodStatusDescription -> rtt_period_status_description\n  Notes -> notes"
    )
}}
select
    "SK_RTTPeriodStatusID" as sk_rtt_period_status_id,
    "BK_RTTPeriodStatusCode" as bk_rtt_period_status_code,
    "RTTPeriodStatusCategory" as rtt_period_status_category,
    "RTTPeriodStatusDescription" as rtt_period_status_description,
    "Notes" as notes
from {{ source('dictionary_dbo', 'RTTPeriodStatus') }}
