-- Staging model for dictionary_dbo.F&AMapping
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_PODID" as sk_podid,
    "SK_LevelID" as sk_level_id,
    "Finance/Activity" as finance_activity,
    "Description" as description
from {{ source('dictionary_dbo', 'F&AMapping') }}
