-- Staging model for dictionary.CarerSupportIndicator
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_CarerSupportIndicatorID" as sk_carersupportindicatorid,
    "BK_CarerSupportIndicator" as bk_carersupportindicator,
    "CarerSupportIndicator" as carersupportindicator
from {{ source('dictionary', 'CarerSupportIndicator') }}
