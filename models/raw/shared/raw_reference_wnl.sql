{{
    config(
        description="Raw layer (Data management reference datasets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.DATA_MANAGEMENT.WNL \ndbt: source(''reference_data_management'', ''WNL'') \nColumns:\n  GP_PRACTICE_CODE -> gp_practice_code\n  Organisation_Name -> organisation_name\n  SUB_ICB_CODE -> sub_icb_code\n  Status -> status\n  Address_Line_1 -> address_line_1\n  Address_Line_2 -> address_line_2\n  Address_Line_3 -> address_line_3\n  Address_Line_4 -> address_line_4\n  Address_Line_5 -> address_line_5\n  Country -> country\n  StartDate -> start_date\n  EndDate -> end_date\n  OrganisationPrimaryRole -> organisation_primary_role\n  Q1_PRACTICE_LIST_SIZE -> q1_practice_list_size\n  Q1_WEIGHTED_LIST_SIZE -> q1_weighted_list_size\n  Q1_LAST_UPDATED -> q1_last_updated"
    )
}}
select
    "GP_PRACTICE_CODE" as gp_practice_code,
    "Organisation_Name" as organisation_name,
    "SUB_ICB_CODE" as sub_icb_code,
    "Status" as status,
    "Address_Line_1" as address_line_1,
    "Address_Line_2" as address_line_2,
    "Address_Line_3" as address_line_3,
    "Address_Line_4" as address_line_4,
    "Address_Line_5" as address_line_5,
    "Country" as country,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "OrganisationPrimaryRole" as organisation_primary_role,
    "Q1_PRACTICE_LIST_SIZE" as q1_practice_list_size,
    "Q1_WEIGHTED_LIST_SIZE" as q1_weighted_list_size,
    "Q1_LAST_UPDATED" as q1_last_updated
from {{ source('reference_data_management', 'WNL') }}
