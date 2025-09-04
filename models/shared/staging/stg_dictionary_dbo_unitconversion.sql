-- Staging model for dictionary_dbo.UnitConversion
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_UnitID_Source" as sk_unit_id_source,
    "SK_UnitID_Target" as sk_unit_id_target,
    "Subtrahend" as subtrahend,
    "Multiplier" as multiplier,
    "Divisor" as divisor,
    "Addend" as addend
from {{ source('dictionary_dbo', 'UnitConversion') }}
