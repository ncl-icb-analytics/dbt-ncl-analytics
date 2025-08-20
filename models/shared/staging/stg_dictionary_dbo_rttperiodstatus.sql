-- Staging model for dictionary_dbo.RTTPeriodStatus
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_RTTPeriodStatusID" as sk_rttperiodstatusid,
    "BK_RTTPeriodStatusCode" as bk_rttperiodstatuscode,
    "RTTPeriodStatusCategory" as rttperiodstatuscategory,
    "RTTPeriodStatusDescription" as rttperiodstatusdescription,
    "Notes" as notes
from {{ source('dictionary_dbo', 'RTTPeriodStatus') }}
