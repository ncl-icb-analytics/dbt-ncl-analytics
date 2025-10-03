-- Raw layer model for dictionary_dbo.OutputArea
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_OutputAreaID" as sk_output_area_id,
    "SK_OutputAreaParentID" as sk_output_area_parent_id,
    "CensusYear" as census_year,
    "OACode" as oa_code,
    "OAName" as oa_name,
    "OAType" as oa_type,
    "GeoEasting" as geo_easting,
    "GeoNorthing" as geo_northing,
    "GeoLatitude" as geo_latitude,
    "GeoLongitude" as geo_longitude,
    "GeoCentroid_text" as geo_centroid_text,
    "GeoCentroid" as geo_centroid,
    "PopEasting" as pop_easting,
    "PopNorthing" as pop_northing,
    "PopLatitude" as pop_latitude,
    "PopLongitude" as pop_longitude,
    "PopCentroid_text" as pop_centroid_text,
    "PopCentroid" as pop_centroid,
    "OAShape_text" as oa_shape_text,
    "OAShape" as oa_shape,
    "TownsendScore" as townsend_score
from {{ source('dictionary_dbo', 'OutputArea') }}
