-- Staging model for dictionary_dbo.UnitMapping
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_UnitID" as sk_unitid,
    "UnitLabel" as unitlabel
from {{ source('dictionary_dbo', 'UnitMapping') }}
