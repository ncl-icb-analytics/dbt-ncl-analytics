-- Staging model for dictionary_dbo.Unit
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_UnitID" as sk_unit_id,
    "UnitSymbol" as unit_symbol,
    "UnitName" as unit_name,
    "QuantityName" as quantity_name,
    "SIPower" as si_power,
    "IsStandardUnit" as is_standard_unit,
    "IsDerivedUnit" as is_derived_unit
from {{ source('dictionary_dbo', 'Unit') }}
