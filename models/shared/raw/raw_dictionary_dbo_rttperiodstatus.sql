-- Raw layer model for dictionary_dbo.RTTPeriodStatus
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_RTTPeriodStatusID" as sk_rtt_period_status_id,
    "BK_RTTPeriodStatusCode" as bk_rtt_period_status_code,
    "RTTPeriodStatusCategory" as rtt_period_status_category,
    "RTTPeriodStatusDescription" as rtt_period_status_description,
    "Notes" as notes
from {{ source('dictionary_dbo', 'RTTPeriodStatus') }}
