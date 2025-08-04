-- Staging model for dictionary.Unit
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_UnitID" as sk_unitid,
    "UnitSymbol" as unitsymbol,
    "UnitName" as unitname,
    "QuantityName" as quantityname,
    "SIPower" as sipower,
    "IsStandardUnit" as isstandardunit,
    "IsDerivedUnit" as isderivedunit
from {{ source('dictionary', 'Unit') }}
