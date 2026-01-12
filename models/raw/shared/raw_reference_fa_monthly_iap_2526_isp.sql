{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.FA__MONTHLY_IAP_2526_ISP \ndbt: source(''reference_analyst_managed'', ''FA__MONTHLY_IAP_2526_ISP'') \nColumns:\n  METRIC -> metric\n  FINANCIAL_MONTH -> financial_month\n  FINANCIAL_YEAR -> financial_year\n  ORGANISATION_IDENTIFIER_(CODE_OF_PROVIDER) -> organisation_identifier_code_of_provider\n  PROVIDER_NAME -> provider_name\n  ORGANISATION_IDENTIFIER_(GP_PRACTICE_RESPONSIBILITY) -> organisation_identifier_gp_practice_responsibility\n  ORGANISATION_IDENTIFIER_(CODE_OF_COMMISSIONER) -> organisation_identifier_code_of_commissioner\n  GENERAL_MEDICAL_PRACTICE_CODE_(PATIENT_REGISTRATION) -> general_medical_practice_code_patient_registration\n  ACTIVITY_TREATMENT_FUNCTION_CODE -> activity_treatment_function_code\n  SPECIALTY_NAME -> specialty_name\n  COMMISSIONED_SERVICE_CATEGORY_CODE -> commissioned_service_category_code\n  POINT_OF_DELIVERY_CODE -> point_of_delivery_code\n  LOCAL_POD_CODE -> local_pod_code\n  LOCAL_POD_DESCRIPTION -> local_pod_description\n  TARIFF_CODE -> tariff_code\n  NATIONAL_TARIFF_INDICATOR -> national_tariff_indicator\n  CONTRACT_MONITORING_PLANNED_ACTIVITY -> contract_monitoring_planned_activity\n  CONTRACT_MONITORING_PLANNED_PRICE -> contract_monitoring_planned_price\n  CONTRACT_MONITORING_PLANNED_MARKET_FORCES_FACTOR -> contract_monitoring_planned_market_forces_factor\n  CONTRACT_MONITORING_ACTUAL_ACTIVITY -> contract_monitoring_actual_activity\n  CONTRACT_MONITORING_ACTUAL_PRICE -> contract_monitoring_actual_price\n  CONTRACT_MONITORING_ACTUAL_MARKET_FORCES_FACTOR -> contract_monitoring_actual_market_forces_factor\n  ORGANISATION_IDENTIFIER_(RESIDENCE_RESPONSIBILITY) -> organisation_identifier_residence_responsibility\n  ORGANISATION_SITE_IDENTIFIER_(OF_TREATMENT) -> organisation_site_identifier_of_treatment\n  MAIN_SPECIALTY_CODE -> main_specialty_code\n  MAIN_SPECIALTY_DESCRIPTION -> main_specialty_description\n  TARIFF_CODE_DESCRIPTION -> tariff_code_description\n  HEALTHCARE_RESOURCE_GROUP_CODE -> healthcare_resource_group_code\n  HRG_DESCRIPTION -> hrg_description\n  RECORD_IDENTIFIER -> record_identifier\n  REPORTING_TYPE -> reporting_type\n  DV_RECIPIENT_CODE -> dv_recipient_code\n  CCG_NAME -> ccg_name\n  LOCAL_POD_GROUPING -> local_pod_grouping"
    )
}}
select
    "METRIC" as metric,
    "FINANCIAL_MONTH" as financial_month,
    "FINANCIAL_YEAR" as financial_year,
    "ORGANISATION_IDENTIFIER_(CODE_OF_PROVIDER)" as organisation_identifier_code_of_provider,
    "PROVIDER_NAME" as provider_name,
    "ORGANISATION_IDENTIFIER_(GP_PRACTICE_RESPONSIBILITY)" as organisation_identifier_gp_practice_responsibility,
    "ORGANISATION_IDENTIFIER_(CODE_OF_COMMISSIONER)" as organisation_identifier_code_of_commissioner,
    "GENERAL_MEDICAL_PRACTICE_CODE_(PATIENT_REGISTRATION)" as general_medical_practice_code_patient_registration,
    "ACTIVITY_TREATMENT_FUNCTION_CODE" as activity_treatment_function_code,
    "SPECIALTY_NAME" as specialty_name,
    "COMMISSIONED_SERVICE_CATEGORY_CODE" as commissioned_service_category_code,
    "POINT_OF_DELIVERY_CODE" as point_of_delivery_code,
    "LOCAL_POD_CODE" as local_pod_code,
    "LOCAL_POD_DESCRIPTION" as local_pod_description,
    "TARIFF_CODE" as tariff_code,
    "NATIONAL_TARIFF_INDICATOR" as national_tariff_indicator,
    "CONTRACT_MONITORING_PLANNED_ACTIVITY" as contract_monitoring_planned_activity,
    "CONTRACT_MONITORING_PLANNED_PRICE" as contract_monitoring_planned_price,
    "CONTRACT_MONITORING_PLANNED_MARKET_FORCES_FACTOR" as contract_monitoring_planned_market_forces_factor,
    "CONTRACT_MONITORING_ACTUAL_ACTIVITY" as contract_monitoring_actual_activity,
    "CONTRACT_MONITORING_ACTUAL_PRICE" as contract_monitoring_actual_price,
    "CONTRACT_MONITORING_ACTUAL_MARKET_FORCES_FACTOR" as contract_monitoring_actual_market_forces_factor,
    "ORGANISATION_IDENTIFIER_(RESIDENCE_RESPONSIBILITY)" as organisation_identifier_residence_responsibility,
    "ORGANISATION_SITE_IDENTIFIER_(OF_TREATMENT)" as organisation_site_identifier_of_treatment,
    "MAIN_SPECIALTY_CODE" as main_specialty_code,
    "MAIN_SPECIALTY_DESCRIPTION" as main_specialty_description,
    "TARIFF_CODE_DESCRIPTION" as tariff_code_description,
    "HEALTHCARE_RESOURCE_GROUP_CODE" as healthcare_resource_group_code,
    "HRG_DESCRIPTION" as hrg_description,
    "RECORD_IDENTIFIER" as record_identifier,
    "REPORTING_TYPE" as reporting_type,
    "DV_RECIPIENT_CODE" as dv_recipient_code,
    "CCG_NAME" as ccg_name,
    "LOCAL_POD_GROUPING" as local_pod_grouping
from {{ source('reference_analyst_managed', 'FA__MONTHLY_IAP_2526_ISP') }}
