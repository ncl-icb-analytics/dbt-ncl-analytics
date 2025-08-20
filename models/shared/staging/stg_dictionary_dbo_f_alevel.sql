-- Staging model for dictionary_dbo.F&ALevel
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_LevelID" as sk_levelid,
    "LevelName" as levelname
from {{ source('dictionary_dbo', 'F&ALevel') }}
