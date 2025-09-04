-- Staging model for dictionary_dbo.PostcodeRegions
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "OSNorthing" as os_northing,
    "Longitude" as longitude,
    "Latitude" as latitude,
    "GeoPoint_text" as geo_point_text,
    "GeoPoint" as geo_point,
    "LocalAuthority" as local_authority,
    "SK_PostcodeID" as sk_postcode_id,
    "Postcode" as postcode,
    "PostcodeFixed" as postcode_fixed,
    "PostcodeNoSpace" as postcode_no_space,
    "OSEasting" as os_easting,
    "WardCode" as ward_code,
    "PostcodeUser" as postcode_user,
    "SHA" as sha,
    "Region" as region,
    "Commissioner" as commissioner,
    "PCT" as pct,
    "LSOACode" as lsoa_code,
    "MSOACode" as msoa_code,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'PostcodeRegions') }}
