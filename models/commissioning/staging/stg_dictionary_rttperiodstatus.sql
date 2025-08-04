-- Staging model for dictionary.RTTPeriodStatus
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_RTTPeriodStatusID" as sk_rttperiodstatusid,
    "BK_RTTPeriodStatusCode" as bk_rttperiodstatuscode,
    "RTTPeriodStatusCategory" as rttperiodstatuscategory,
    "RTTPeriodStatusDescription" as rttperiodstatusdescription,
    "Notes" as notes
from {{ source('dictionary', 'RTTPeriodStatus') }}
