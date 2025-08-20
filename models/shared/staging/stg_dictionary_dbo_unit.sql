-- Staging model for dictionary_dbo.Unit
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_UnitID" as sk_unitid,
    "UnitSymbol" as unitsymbol,
    "UnitName" as unitname,
    "QuantityName" as quantityname,
    "SIPower" as sipower,
    "IsStandardUnit" as isstandardunit,
    "IsDerivedUnit" as isderivedunit
from {{ source('dictionary_dbo', 'Unit') }}
