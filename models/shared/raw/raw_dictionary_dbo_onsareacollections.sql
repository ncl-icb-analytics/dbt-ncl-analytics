-- Raw layer model for dictionary_dbo.ONSAreaCollections
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_AreaCollectionID" as sk_area_collection_id,
    "AreaCode" as area_code,
    "AreaName" as area_name,
    "Country" as country
from {{ source('dictionary_dbo', 'ONSAreaCollections') }}
