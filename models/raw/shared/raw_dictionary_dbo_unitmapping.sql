-- Raw layer model for dictionary_dbo.UnitMapping
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_UnitID" as sk_unit_id,
    "UnitLabel" as unit_label
from {{ source('dictionary_dbo', 'UnitMapping') }}
