-- Staging model for dictionary_dbo.PostcodeRegions
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "OSNorthing" as osnorthing,
    "Longitude" as longitude,
    "Latitude" as latitude,
    "GeoPoint_text" as geopoint_text,
    "GeoPoint" as geopoint,
    "LocalAuthority" as localauthority,
    "SK_PostcodeID" as sk_postcodeid,
    "Postcode" as postcode,
    "PostcodeFixed" as postcodefixed,
    "PostcodeNoSpace" as postcodenospace,
    "OSEasting" as oseasting,
    "WardCode" as wardcode,
    "PostcodeUser" as postcodeuser,
    "SHA" as sha,
    "Region" as region,
    "Commissioner" as commissioner,
    "PCT" as pct,
    "LSOACode" as lsoacode,
    "MSOACode" as msoacode,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_dbo', 'PostcodeRegions') }}
