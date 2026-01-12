{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.FA__MONTHLY_FA_ACM_VERSIONS_V3 \ndbt: source(''reference_analyst_managed'', ''FA__MONTHLY_FA_ACM_VERSIONS_V3'') \nColumns:\n  POINT OF DELIVERY CODE -> point_of_delivery_code\n  LOCAL POINT OF DELIVERY CODE -> local_point_of_delivery_code\n  PODGroupOverview -> pod_group_overview\n  POINT OF DELIVERY FURTHER DETAIL CODE -> point_of_delivery_further_detail_code\n  LOCAL POINT OF DELIVERY DESCRIPTION -> local_point_of_delivery_description\n  POINT OF DELIVERY FURTHER DETAIL DESCRIPTION -> point_of_delivery_further_detail_description\n  TARIFF CODE -> tariff_code\n  CONTRACT MONITORING ACTUAL MARKET FORCES FACTOR -> contract_monitoring_actual_market_forces_factor\n  ACTIVITY TREATMENT FUNCTION CODE -> activity_treatment_function_code\n  MAIN SPECIALTY CODE -> main_specialty_code\n  LOCAL SUB-SPECIALTY CODE -> local_sub_specialty_code\n  FinMonth -> fin_month\n  DV_FINANCIALMONTH -> dv_financialmonth\n  DV_FINANCIALYEAR -> dv_financialyear\n  FINANCIALYEAR -> financialyear\n  ORGANISATION IDENTIFIER (CODE OF COMMISSIONER) -> organisation_identifier_code_of_commissioner\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  OLD PROVIDER NAMES -> old_provider_names\n  DV_REPORTING TYPE -> dv_reporting_type\n  Organisation_Name -> organisation_name\n  NCL_FLAG -> ncl_flag\n  COMMISSIONED SERVICE CATEGORY CODE -> commissioned_service_category_code\n  SERVICE CODE -> service_code\n  ACTIVITY -> activity\n  PRICE -> price"
    )
}}
select
    "POINT OF DELIVERY CODE" as point_of_delivery_code,
    "LOCAL POINT OF DELIVERY CODE" as local_point_of_delivery_code,
    "PODGroupOverview" as pod_group_overview,
    "POINT OF DELIVERY FURTHER DETAIL CODE" as point_of_delivery_further_detail_code,
    "LOCAL POINT OF DELIVERY DESCRIPTION" as local_point_of_delivery_description,
    "POINT OF DELIVERY FURTHER DETAIL DESCRIPTION" as point_of_delivery_further_detail_description,
    "TARIFF CODE" as tariff_code,
    "CONTRACT MONITORING ACTUAL MARKET FORCES FACTOR" as contract_monitoring_actual_market_forces_factor,
    "ACTIVITY TREATMENT FUNCTION CODE" as activity_treatment_function_code,
    "MAIN SPECIALTY CODE" as main_specialty_code,
    "LOCAL SUB-SPECIALTY CODE" as local_sub_specialty_code,
    "FinMonth" as fin_month,
    "DV_FINANCIALMONTH" as dv_financialmonth,
    "DV_FINANCIALYEAR" as dv_financialyear,
    "FINANCIALYEAR" as financialyear,
    "ORGANISATION IDENTIFIER (CODE OF COMMISSIONER)" as organisation_identifier_code_of_commissioner,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "OLD PROVIDER NAMES" as old_provider_names,
    "DV_REPORTING TYPE" as dv_reporting_type,
    "Organisation_Name" as organisation_name,
    "NCL_FLAG" as ncl_flag,
    "COMMISSIONED SERVICE CATEGORY CODE" as commissioned_service_category_code,
    "SERVICE CODE" as service_code,
    "ACTIVITY" as activity,
    "PRICE" as price
from {{ source('reference_analyst_managed', 'FA__MONTHLY_FA_ACM_VERSIONS_V3') }}
