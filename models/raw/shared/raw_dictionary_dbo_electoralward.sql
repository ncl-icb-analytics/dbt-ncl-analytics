{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.ElectoralWard \ndbt: source(''dictionary_dbo'', ''ElectoralWard'') \nColumns:\n  SK_ElectoralWardID -> sk_electoral_ward_id\n  CountryCode -> country_code\n  CountryName -> country_name\n  National_Grouping_Code -> national_grouping_code\n  Health_Board_Local_Health_Board_Strategic_Authority_Name -> health_board_local_health_board_strategic_authority_name\n  High_Level_Health_Authority_Code -> high_level_health_authority_code\n  Local_Health_Board_Code_Wales -> local_health_board_code_wales\n  Local_Health_Board_Name -> local_health_board_name\n  ONS_LA_UA_Code_old -> ons_la_ua_code_old\n  ONS_LA_UA_Code_9char -> ons_la_ua_code_9char\n  Ward_Name -> ward_name\n  ONS_Ward_Code_old -> ons_ward_code_old\n  ONS_Ward_Code_9char -> ons_ward_code_9char\n  SK_ElectoralWard_ID -> sk_electoral_ward_id_1\n  IsActive -> is_active"
    )
}}
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
