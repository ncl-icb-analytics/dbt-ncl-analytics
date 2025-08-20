-- Staging model for dictionary_dbo.CostBand
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_CostBandID" as sk_costbandid,
    "CostBandLabel" as costbandlabel,
    "CostBandStart" as costbandstart,
    "CostBandEnd" as costbandend
from {{ source('dictionary_dbo', 'CostBand') }}
