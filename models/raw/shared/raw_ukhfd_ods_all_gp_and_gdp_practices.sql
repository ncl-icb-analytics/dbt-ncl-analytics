{{
    config(
        description="Raw layer (UKHFD national ODS organisation reference). 1:1 passthrough with cleaned column names. \nSource: UKHFD.ODS.dim_All_GP_and_GDP_Practices_SCD \ndbt: source(''ukhfd_ods'', ''all_gp_and_gdp_practices'')"
    )
}}
select
    "Organisation_Code" as organisation_code,
    "Organisation_Name" as organisation_name,
    "Postcode" as postcode,
    "Open_Date" as open_date,
    "Close_Date" as close_date,
    "Status_Code" as status_code,
    "Parent_Organisation_Code" as parent_organisation_code,
    "Prescribing_Setting" as prescribing_setting,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_ods', 'all_gp_and_gdp_practices') }}
