-- Staging model for dictionary.ONSAreaCollections
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_AreaCollectionID" as sk_areacollectionid,
    "AreaCode" as areacode,
    "AreaName" as areaname,
    "Country" as country
from {{ source('dictionary', 'ONSAreaCollections') }}
