-- Raw layer model for dictionary_dbo.F&AMapping
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_PODID" as sk_podid,
    "SK_LevelID" as sk_level_id,
    "Finance/Activity" as finance_activity,
    "Description" as description
from {{ source('dictionary_dbo', 'F&AMapping') }}
