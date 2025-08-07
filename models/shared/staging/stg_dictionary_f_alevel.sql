-- Staging model for dictionary.F&ALevel
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_LevelID" as sk_levelid,
    "LevelName" as levelname
from {{ source('dictionary', 'F&ALevel') }}
