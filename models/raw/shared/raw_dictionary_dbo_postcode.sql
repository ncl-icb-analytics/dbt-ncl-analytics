{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Postcode \ndbt: source(''dictionary_dbo'', ''Postcode'') \nColumns:\n  SK_PostcodeID -> sk_postcode_id\n  Postcode_8_chars -> postcode_8_chars\n  Postcode_single_space_e_Gif -> postcode_single_space_e_gif\n  Postcode_no_space -> postcode_no_space\n  Date_of_Introduction -> date_of_introduction\n  Date_of_Termination -> date_of_termination\n  Grid_Ref_Easting -> grid_ref_easting\n  Grid_Ref_Northing -> grid_ref_northing\n  Local_Authority_District_Unitary_Authority -> local_authority_district_unitary_authority\n  Electoral_Ward_or_Division -> electoral_ward_or_division\n  Strategic_Health_Authority -> strategic_health_authority\n  Primary_Care_Organisation -> primary_care_organisation\n  yr1998_Ward_Code -> yr1998_ward_code\n  Old_PCT -> old_pct\n  yr2001_LSOA -> yr2001_lsoa\n  yr2001_MSOA -> yr2001_msoa\n  yr2011_OA -> yr2011_oa\n  yr2011_LSOA -> yr2011_lsoa\n  yr2011_MSOA -> yr2011_msoa\n  Latitude -> latitude\n  Longitude -> longitude\n  FirstCreated -> first_created\n  LastUpdated -> last_updated\n  GOR_Code -> gor_code\n  Postcode_Usertype -> postcode_usertype\n  Area_Team_Code -> area_team_code\n  Postcode -> postcode\n  LSOA -> lsoa\n  MSOA -> msoa\n  SK_PseudoPostcodeID -> sk_pseudo_postcode_id\n  SK_Postcode_ID -> sk_postcode_id_1"
    )
}}
select
    "SK_PostcodeID" as sk_postcode_id,
    "Postcode_8_chars" as postcode_8_chars,
    "Postcode_single_space_e_Gif" as postcode_single_space_e_gif,
    "Postcode_no_space" as postcode_no_space,
    "Date_of_Introduction" as date_of_introduction,
    "Date_of_Termination" as date_of_termination,
    "Grid_Ref_Easting" as grid_ref_easting,
    "Grid_Ref_Northing" as grid_ref_northing,
    "Local_Authority_District_Unitary_Authority" as local_authority_district_unitary_authority,
    "Electoral_Ward_or_Division" as electoral_ward_or_division,
    "Strategic_Health_Authority" as strategic_health_authority,
    "Primary_Care_Organisation" as primary_care_organisation,
    "yr1998_Ward_Code" as yr1998_ward_code,
    "Old_PCT" as old_pct,
    "yr2001_LSOA" as yr2001_lsoa,
    "yr2001_MSOA" as yr2001_msoa,
    "yr2011_OA" as yr2011_oa,
    "yr2011_LSOA" as yr2011_lsoa,
    "yr2011_MSOA" as yr2011_msoa,
    "Latitude" as latitude,
    "Longitude" as longitude,
    "FirstCreated" as first_created,
    "LastUpdated" as last_updated,
    "GOR_Code" as gor_code,
    "Postcode_Usertype" as postcode_usertype,
    "Area_Team_Code" as area_team_code,
    "Postcode" as postcode,
    "LSOA" as lsoa,
    "MSOA" as msoa,
    "SK_PseudoPostcodeID" as sk_pseudo_postcode_id,
    "SK_Postcode_ID" as sk_postcode_id_1
from {{ source('dictionary_dbo', 'Postcode') }}
