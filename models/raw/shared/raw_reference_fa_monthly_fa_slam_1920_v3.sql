{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.FA__MONTHLY_FA_SLAM_1920_V3 \ndbt: source(''reference_analyst_managed'', ''FA__MONTHLY_FA_SLAM_1920_V3'') \nColumns:\n  SLAMPOD -> slampod\n  LOCAL POINT OF DELIVERY CODE -> local_point_of_delivery_code\n  PODGroupOverview -> pod_group_overview\n  PODGroup -> pod_group\n  SLAMPODDescription -> slampod_description\n  PODGroupDescription -> pod_group_description\n  SLAMHRGCode -> slamhrg_code\n  CONTRACT MONITORING ACTUAL MARKET FORCES FACTOR -> contract_monitoring_actual_market_forces_factor\n  SLAMSpecialtyCode -> slam_specialty_code\n  MAIN SPECIALTY CODE -> main_specialty_code\n  LOCAL SUB SPECIALTY CODE -> local_sub_specialty_code\n  FinMonth -> fin_month\n  DV_FINANCIALMONTH -> dv_financialmonth\n  DV_FINANCIALYEAR -> dv_financialyear\n  FinancialYear -> financial_year\n  CommissionerID -> commissioner_id\n  ServiceProviderID -> service_provider_id\n  OLD PROVIDER NAMES -> old_provider_names\n  Reporting Type -> reporting_type\n  ServiceProviderDescription -> service_provider_description\n  NCL_FLag -> ncl_flag\n  COMMISSIONED SERVICE CATEGORY CODE -> commissioned_service_category_code\n  SERVICE CODE -> service_code\n  ACTIVITY -> activity\n  PRICE -> price"
    )
}}
select
    "SLAMPOD" as slampod,
    "LOCAL POINT OF DELIVERY CODE" as local_point_of_delivery_code,
    "PODGroupOverview" as pod_group_overview,
    "PODGroup" as pod_group,
    "SLAMPODDescription" as slampod_description,
    "PODGroupDescription" as pod_group_description,
    "SLAMHRGCode" as slamhrg_code,
    "CONTRACT MONITORING ACTUAL MARKET FORCES FACTOR" as contract_monitoring_actual_market_forces_factor,
    "SLAMSpecialtyCode" as slam_specialty_code,
    "MAIN SPECIALTY CODE" as main_specialty_code,
    "LOCAL SUB SPECIALTY CODE" as local_sub_specialty_code,
    "FinMonth" as fin_month,
    "DV_FINANCIALMONTH" as dv_financialmonth,
    "DV_FINANCIALYEAR" as dv_financialyear,
    "FinancialYear" as financial_year,
    "CommissionerID" as commissioner_id,
    "ServiceProviderID" as service_provider_id,
    "OLD PROVIDER NAMES" as old_provider_names,
    "Reporting Type" as reporting_type,
    "ServiceProviderDescription" as service_provider_description,
    "NCL_FLag" as ncl_flag,
    "COMMISSIONED SERVICE CATEGORY CODE" as commissioned_service_category_code,
    "SERVICE CODE" as service_code,
    "ACTIVITY" as activity,
    "PRICE" as price
from {{ source('reference_analyst_managed', 'FA__MONTHLY_FA_SLAM_1920_V3') }}
