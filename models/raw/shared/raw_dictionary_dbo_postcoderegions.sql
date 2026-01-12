{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.PostcodeRegions \ndbt: source(''dictionary_dbo'', ''PostcodeRegions'') \nColumns:\n  OSNorthing -> os_northing\n  Longitude -> longitude\n  Latitude -> latitude\n  GeoPoint_text -> geo_point_text\n  GeoPoint -> geo_point\n  LocalAuthority -> local_authority\n  SK_PostcodeID -> sk_postcode_id\n  Postcode -> postcode\n  PostcodeFixed -> postcode_fixed\n  PostcodeNoSpace -> postcode_no_space\n  OSEasting -> os_easting\n  WardCode -> ward_code\n  PostcodeUser -> postcode_user\n  SHA -> sha\n  Region -> region\n  Commissioner -> commissioner\n  PCT -> pct\n  LSOACode -> lsoa_code\n  MSOACode -> msoa_code\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
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
