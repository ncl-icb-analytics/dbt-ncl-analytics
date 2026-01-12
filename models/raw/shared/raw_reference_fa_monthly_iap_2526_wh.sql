{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.FA__MONTHLY_IAP_2526_WH \ndbt: source(''reference_analyst_managed'', ''FA__MONTHLY_IAP_2526_WH'') \nColumns:\n  FINANCIAL_MONTH -> financial_month\n  FINANCIAL_YEAR -> financial_year\n  DATE_AND_TIME_DATA_SET_CREATED -> date_and_time_data_set_created\n  ORGANISATION_IDENTIFIER_(CODE_OF_PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION_SITE_IDENTIFIER_(OF_TREATMENT) -> organisation_site_identifier_of_treatment\n  ORGANISATION_IDENTIFIER_(GP_PRACTICE_RESPONSIBILITY) -> organisation_identifier_gp_practice_responsibility\n  ORGANISATION_IDENTIFIER_(RESIDENCE_RESPONSIBILITY) -> organisation_identifier_residence_responsibility\n  ORGANISATION_IDENTIFIER_(CODE_OF_COMMISSIONER) -> organisation_identifier_code_of_commissioner\n  GENERAL_MEDICAL_PRACTICE_(PATIENT_REGISTRATION) -> general_medical_practice_patient_registration\n  ACTIVITY_TREATMENT_FUNCTION_CODE -> activity_treatment_function_code\n  LOCAL_SUB-SPECIALTY_CODE -> local_sub_specialty_code\n  WARD_CODE -> ward_code\n  COMMISSIONED_SERVICE_CATEGORY_CODE -> commissioned_service_category_code\n  SERVICE_CODE -> service_code\n  SPECIALISED_MENTAL_HEALTH_SERVICE_CATEGORY_CODE -> specialised_mental_health_service_category_code\n  POINT_OF_DELIVERY_CODE -> point_of_delivery_code\n  POINT_OF_DELIVERY_FURTHER_DETAIL_CODE -> point_of_delivery_further_detail_code\n  POINT_OF_DELIVERY_FURTHER_DETAIL_DESCRIPTION -> point_of_delivery_further_detail_description\n  LOCAL_POINT_OF_DELIVERY_CODE -> local_point_of_delivery_code\n  LOCAL_POINT_OF_DELIVERY_DESCRIPTION -> local_point_of_delivery_description\n  LOCAL_CONTRACT_CODE -> local_contract_code\n  LOCAL_CONTRACT_CODE_DESCRIPTION -> local_contract_code_description\n  LOCAL_CONTRACT_MONITORING_CODE -> local_contract_monitoring_code\n  LOCAL_CONTRACT_MONITORING_DESCRIPTION -> local_contract_monitoring_description\n  CONTRACT_MONITORING_ADDITIONAL_DETAIL -> contract_monitoring_additional_detail\n  CONTRACT_MONITORING_ADDITIONAL_DESCRIPTION -> contract_monitoring_additional_description\n  TARIFF_CODE -> tariff_code\n  NATIONAL_TARIFF_INDICATOR -> national_tariff_indicator\n  CONTRACT_MONITORING_PLANNED_ACTIVITY -> contract_monitoring_planned_activity\n  CONTRACT_MONITORING_PLANNED_PRICE -> contract_monitoring_planned_price\n  CONTRACT_MONITORING_PLANNED_MARKET_FORCES_FACTOR -> contract_monitoring_planned_market_forces_factor\n  NAME_OF_SUBMITTER -> name_of_submitter"
    )
}}
select
    "FINANCIAL_MONTH" as financial_month,
    "FINANCIAL_YEAR" as financial_year,
    "DATE_AND_TIME_DATA_SET_CREATED" as date_and_time_data_set_created,
    "ORGANISATION_IDENTIFIER_(CODE_OF_PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION_SITE_IDENTIFIER_(OF_TREATMENT)" as organisation_site_identifier_of_treatment,
    "ORGANISATION_IDENTIFIER_(GP_PRACTICE_RESPONSIBILITY)" as organisation_identifier_gp_practice_responsibility,
    "ORGANISATION_IDENTIFIER_(RESIDENCE_RESPONSIBILITY)" as organisation_identifier_residence_responsibility,
    "ORGANISATION_IDENTIFIER_(CODE_OF_COMMISSIONER)" as organisation_identifier_code_of_commissioner,
    "GENERAL_MEDICAL_PRACTICE_(PATIENT_REGISTRATION)" as general_medical_practice_patient_registration,
    "ACTIVITY_TREATMENT_FUNCTION_CODE" as activity_treatment_function_code,
    "LOCAL_SUB-SPECIALTY_CODE" as local_sub_specialty_code,
    "WARD_CODE" as ward_code,
    "COMMISSIONED_SERVICE_CATEGORY_CODE" as commissioned_service_category_code,
    "SERVICE_CODE" as service_code,
    "SPECIALISED_MENTAL_HEALTH_SERVICE_CATEGORY_CODE" as specialised_mental_health_service_category_code,
    "POINT_OF_DELIVERY_CODE" as point_of_delivery_code,
    "POINT_OF_DELIVERY_FURTHER_DETAIL_CODE" as point_of_delivery_further_detail_code,
    "POINT_OF_DELIVERY_FURTHER_DETAIL_DESCRIPTION" as point_of_delivery_further_detail_description,
    "LOCAL_POINT_OF_DELIVERY_CODE" as local_point_of_delivery_code,
    "LOCAL_POINT_OF_DELIVERY_DESCRIPTION" as local_point_of_delivery_description,
    "LOCAL_CONTRACT_CODE" as local_contract_code,
    "LOCAL_CONTRACT_CODE_DESCRIPTION" as local_contract_code_description,
    "LOCAL_CONTRACT_MONITORING_CODE" as local_contract_monitoring_code,
    "LOCAL_CONTRACT_MONITORING_DESCRIPTION" as local_contract_monitoring_description,
    "CONTRACT_MONITORING_ADDITIONAL_DETAIL" as contract_monitoring_additional_detail,
    "CONTRACT_MONITORING_ADDITIONAL_DESCRIPTION" as contract_monitoring_additional_description,
    "TARIFF_CODE" as tariff_code,
    "NATIONAL_TARIFF_INDICATOR" as national_tariff_indicator,
    "CONTRACT_MONITORING_PLANNED_ACTIVITY" as contract_monitoring_planned_activity,
    "CONTRACT_MONITORING_PLANNED_PRICE" as contract_monitoring_planned_price,
    "CONTRACT_MONITORING_PLANNED_MARKET_FORCES_FACTOR" as contract_monitoring_planned_market_forces_factor,
    "NAME_OF_SUBMITTER" as name_of_submitter
from {{ source('reference_analyst_managed', 'FA__MONTHLY_IAP_2526_WH') }}
