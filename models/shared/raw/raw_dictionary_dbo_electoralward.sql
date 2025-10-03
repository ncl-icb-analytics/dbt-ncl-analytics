-- Raw layer model for dictionary_dbo.ElectoralWard
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ElectoralWardID" as sk_electoral_ward_id,
    "CountryCode" as country_code,
    "CountryName" as country_name,
    "National_Grouping_Code" as national_grouping_code,
    "Health_Board_Local_Health_Board_Strategic_Authority_Name" as health_board_local_health_board_strategic_authority_name,
    "High_Level_Health_Authority_Code" as high_level_health_authority_code,
    "Local_Health_Board_Code_Wales" as local_health_board_code_wales,
    "Local_Health_Board_Name" as local_health_board_name,
    "ONS_LA_UA_Code_old" as ons_la_ua_code_old,
    "ONS_LA_UA_Code_9char" as ons_la_ua_code_9char,
    "Ward_Name" as ward_name,
    "ONS_Ward_Code_old" as ons_ward_code_old,
    "ONS_Ward_Code_9char" as ons_ward_code_9char,
    "SK_ElectoralWard_ID" as sk_electoral_ward_id_1,
    "IsActive" as is_active
from {{ source('dictionary_dbo', 'ElectoralWard') }}
