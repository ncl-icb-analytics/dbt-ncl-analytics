-- Staging model for dictionary.UnitConversion
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_UnitID_Source" as sk_unitid_source,
    "SK_UnitID_Target" as sk_unitid_target,
    "Subtrahend" as subtrahend,
    "Multiplier" as multiplier,
    "Divisor" as divisor,
    "Addend" as addend
from {{ source('dictionary', 'UnitConversion') }}
