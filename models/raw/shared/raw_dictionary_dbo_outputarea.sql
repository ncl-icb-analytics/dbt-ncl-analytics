{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OutputArea \ndbt: source(''dictionary_dbo'', ''OutputArea'') \nColumns:\n  SK_OutputAreaID -> sk_output_area_id\n  SK_OutputAreaParentID -> sk_output_area_parent_id\n  CensusYear -> census_year\n  OACode -> oa_code\n  OAName -> oa_name\n  OAType -> oa_type\n  GeoEasting -> geo_easting\n  GeoNorthing -> geo_northing\n  GeoLatitude -> geo_latitude\n  GeoLongitude -> geo_longitude\n  GeoCentroid_text -> geo_centroid_text\n  GeoCentroid -> geo_centroid\n  PopEasting -> pop_easting\n  PopNorthing -> pop_northing\n  PopLatitude -> pop_latitude\n  PopLongitude -> pop_longitude\n  PopCentroid_text -> pop_centroid_text\n  PopCentroid -> pop_centroid\n  OAShape_text -> oa_shape_text\n  OAShape -> oa_shape\n  TownsendScore -> townsend_score"
    )
}}
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
