-- Staging model for dictionary_dbo.Wards
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

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
