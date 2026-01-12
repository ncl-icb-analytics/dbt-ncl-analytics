{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.CostBand \ndbt: source(''dictionary_dbo'', ''CostBand'') \nColumns:\n  SK_CostBandID -> sk_cost_band_id\n  CostBandLabel -> cost_band_label\n  CostBandStart -> cost_band_start\n  CostBandEnd -> cost_band_end"
    )
}}
select
    "SK_CostBandID" as sk_cost_band_id,
    "CostBandLabel" as cost_band_label,
    "CostBandStart" as cost_band_start,
    "CostBandEnd" as cost_band_end
from {{ source('dictionary_dbo', 'CostBand') }}
