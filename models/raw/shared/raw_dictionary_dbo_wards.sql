{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Wards \ndbt: source(''dictionary_dbo'', ''Wards'') \nColumns:\n  SK_WardID -> sk_ward_id\n  WardCode -> ward_code\n  WardName -> ward_name\n  GeoCentroid_text -> geo_centroid_text\n  GeoCentroid -> geo_centroid\n  WardShape_text -> ward_shape_text\n  WardShape -> ward_shape\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_WardID" as sk_ward_id,
    "WardCode" as ward_code,
    "WardName" as ward_name,
    "GeoCentroid_text" as geo_centroid_text,
    "GeoCentroid" as geo_centroid,
    "WardShape_text" as ward_shape_text,
    "WardShape" as ward_shape,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'Wards') }}
