-- Raw layer model for dictionary_dbo.CarerSupportIndicator
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_CarerSupportIndicatorID" as sk_carer_support_indicator_id,
    "BK_CarerSupportIndicator" as bk_carer_support_indicator,
    "CarerSupportIndicator" as carer_support_indicator
from {{ source('dictionary_dbo', 'CarerSupportIndicator') }}
