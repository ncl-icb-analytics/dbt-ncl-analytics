-- Staging model for dictionary.F&AMapping
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_PODID" as sk_podid,
    "SK_LevelID" as sk_levelid,
    "Finance/Activity" as finance_activity,
    "Description" as description
from {{ source('dictionary', 'F&AMapping') }}
