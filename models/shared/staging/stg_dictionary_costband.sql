-- Staging model for dictionary.CostBand
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_CostBandID" as sk_costbandid,
    "CostBandLabel" as costbandlabel,
    "CostBandStart" as costbandstart,
    "CostBandEnd" as costbandend
from {{ source('dictionary', 'CostBand') }}
