-- Staging model for dictionary_dbo.ONSAreaCollections
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_AreaCollectionID" as sk_areacollectionid,
    "AreaCode" as areacode,
    "AreaName" as areaname,
    "Country" as country
from {{ source('dictionary_dbo', 'ONSAreaCollections') }}
