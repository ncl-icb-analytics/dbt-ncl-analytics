-- Staging model for dictionary_dbo.UnitMapping
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_UnitID" as sk_unit_id,
    "UnitLabel" as unit_label
from {{ source('dictionary_dbo', 'UnitMapping') }}
