{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.ONSAreaCollections \ndbt: source(''dictionary_dbo'', ''ONSAreaCollections'') \nColumns:\n  SK_AreaCollectionID -> sk_area_collection_id\n  AreaCode -> area_code\n  AreaName -> area_name\n  Country -> country"
    )
}}
select
    "SK_AreaCollectionID" as sk_area_collection_id,
    "AreaCode" as area_code,
    "AreaName" as area_name,
    "Country" as country
from {{ source('dictionary_dbo', 'ONSAreaCollections') }}
