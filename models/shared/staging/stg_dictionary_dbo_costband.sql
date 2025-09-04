-- Staging model for dictionary_dbo.CostBand
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_CostBandID" as sk_cost_band_id,
    "CostBandLabel" as cost_band_label,
    "CostBandStart" as cost_band_start,
    "CostBandEnd" as cost_band_end
from {{ source('dictionary_dbo', 'CostBand') }}
