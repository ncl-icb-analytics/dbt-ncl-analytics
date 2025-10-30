-- Raw layer model for reference_analyst_managed.FA__MONTHLY_FA_SLAM_1920_V3
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
