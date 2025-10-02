-- Raw layer model for dictionary_dbo.F&ALevel
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_LevelID" as sk_level_id,
    "LevelName" as level_name
from {{ source('dictionary_dbo', 'F&ALevel') }}
