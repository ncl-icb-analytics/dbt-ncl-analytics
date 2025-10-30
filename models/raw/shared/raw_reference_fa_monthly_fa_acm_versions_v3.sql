-- Raw layer model for reference_analyst_managed.FA__MONTHLY_FA_ACM_VERSIONS_V3
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
